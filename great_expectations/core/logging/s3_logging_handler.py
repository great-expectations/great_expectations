"""
S3 Binding Module with logging handler and stream object.

This handler is modified from the aws_logging_handlers library by Omri Eival
https://github.com/omrikiei/aws_logging_handlers

That library is released under the MIT license:

MIT License

Copyright (c) 2018 Omri Eival

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

import atexit
import signal
import threading
import queue
import gzip
import codecs
from collections import namedtuple

from logging import getLogger, StreamHandler
from io import BufferedIOBase, BytesIO

import botocore
from boto3 import Session
from datetime import datetime

from botocore.config import Config

DEFAULT_CHUNK_SIZE = 5 * 1024 ** 2  # 5 MB
DEFAULT_ROTATION_TIME_SECS = 12 * 60 * 60  # 12 hours
MAX_FILE_SIZE_BYTES = 100 * 1024 ** 2  # 100 MB
MIN_WORKERS_NUM = 1

STOP_SIGNAL = object()

logger = getLogger(__name__)


def is_non_empty_string(s):
    return isinstance(s, str) and s != ""


def empty_str_err(s):
    return "{} should be a non-empty string".format(s)


def is_positive_int(n):
    return isinstance(n, int) and n > 0


def bad_integer_err(n):
    return "{} should be a positive integer".format(n)


def is_boolean(b):
    return isinstance(b, bool)


def bad_type_error(n, t):
    return "{} should be of type {}".format(n, t)


def is_string_func(o):
    return isinstance(o, str) and o or callable(o)


ValidationRule = namedtuple('ValidationRule', ['arg', 'func', 'message'])


class Task:
    def __init__(self, callable_func, *args, **kwargs):
        assert callable(callable_func), "First argument in task should be callable"
        self.callable_func = callable_func
        self.args = args
        self.kwargs = kwargs


def task_worker(q):
    while True:
        task = q.get()
        if task == STOP_SIGNAL:
            return
        assert isinstance(task, (Task,)), "task should be of type `Task` only!"
        task.callable_func(*task.args, **task.kwargs)
        q.task_done()


class StreamObject(object):
    """
    Class representation of the AWS s3 object along with all the needed metadata to stream to s3
    """

    def __init__(self, s3_resource, bucket_name, filename, buffer_queue):
        self.object = s3_resource.Object(bucket_name, filename)
        self.uploader = self.object.initiate_multipart_upload()
        self.bucket = bucket_name
        try:
            total_bytes = s3_resource.meta.client.head_object(Bucket=self.bucket.name, Key=filename)
        except Exception:
            total_bytes = 0

        self.buffer = BytesIO()
        self.chunk_count = 0
        self.byte_count = total_bytes
        self.parts = []
        self.tasks = buffer_queue

    def add_task(self, task):
        """
        Add a task to the tasks queue
        :param task: Task object
        :return:
        """
        self.tasks.put(task)

    def join_tasks(self):
        """
        Join all the tasks
        :return:
        """
        self.tasks.join()


class S3Stream(BufferedIOBase):
    """
    stream interface used by the handler
    """

    _stream_buffer_queue = queue.Queue()
    _rotation_queue = queue.Queue()

    def __init__(self, bucket: str, key: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 max_file_log_time: int = DEFAULT_ROTATION_TIME_SECS, max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
                 encoder: str = 'utf-8', workers: int = 1, compress: bool = False, **boto_session_kwargs):
        """
        :param bucket: name of the s3 bucket
        :type bucket: str
        :param key: s3 key path
        :type key: str
        :param chunk_size: size of multipart upload chunk size (default 5MB)
        :type chunk_size: int
        :param max_file_log_time: threshold period for a log period until file rotation (default 12 Hours)
        :type max_file_log_time: int
        :param max_file_size_bytes: threshold for file rotation by bytes (default 100MB)
        :type max_file_size_bytes: int
        :param encoder: the encoder to be used for log records (default 'utf-8')
        :type encoder: str
        :param workers: the number of background workers that rotate log records (default 1)
        :type workers: int
        :param compress: flag indication for archiving the content of a file
        :type compress: bool
        :param boto_session_kwargs: additional keyword arguments for the AWS Kinesis Resource
        :type boto_session_kwargs: boto3 resource keyword arguments
        """
        super(BufferedIOBase, self).__init__()
        # NOTE - JPC - 20200227 - Note this fairly ungenerous configuration parameter. We just don't wait to wait a
        # a long time to know if things got started.
        config = Config(connect_timeout=1, read_timeout=1, retries={'max_attempts': 1})
        if config in boto_session_kwargs:
            logger.warning("Overwriting provided boto session config.")
        boto_session_kwargs["config"] = config
        self._session = Session()
        self.s3 = self._session.resource('s3', **boto_session_kwargs)
        self.start_time = int(datetime.utcnow().strftime('%s'))
        self.key = key
        self.chunk_size = chunk_size
        self.max_file_log_time = max_file_log_time
        self.max_file_size_bytes = max_file_size_bytes
        self.current_file_name = "{}_{}".format(key, int(datetime.utcnow().strftime('%s')))
        if compress:
            self.current_file_name = "{}.gz".format(self.current_file_name)
        self.encoder = encoder
        self.compress = compress

        # If we cannot open a connection, a ConnectionError will be raised
        self.bucket = bucket
        self._current_object = self._get_stream_object(self.current_file_name)
        self.workers = [threading.Thread(target=task_worker, args=(self._rotation_queue,), daemon=True).start() for _ in
                        range(int(max(workers, MIN_WORKERS_NUM) / 2) + 1)]
        self._stream_bg_workers = [threading.Thread(target=task_worker, args=(self._stream_buffer_queue,), daemon=True).start() for _
                                   in range(max(int(max(workers, MIN_WORKERS_NUM) / 2), 1))]

        self._is_open = True

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, val):
        if not val:
            raise ValueError("Bucket name is invalid")
        try:
            self.s3.meta.client.head_bucket(Bucket=val)
        except (botocore.exceptions.ClientError, botocore.exceptions.EndpointConnectionError):
            # TODO: be more precise about the kind of error
            raise ConnectionError("Unable to connect to bucket: %s bucket does not exist, insufficient "
                                  "permissions, or no network access.")
        except Exception as e:
            logger.warning("Unable to set up S3 telemetry because of an unrecognized error. Going silent." + str(e))
            raise ConnectionError("Unable to set up S3 telemetry. Going silent.")
        self._bucket = self.s3.Bucket(val)

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, val):
        if not val:
            raise ValueError("Given key is invalid")
        self._key = val.strip('/')

    @property
    def encoder(self):
        return self._encoder

    @encoder.setter
    def encoder(self, val):
        _ = codecs.getencoder(val)
        self._encoder = val

    def get_filename(self):
        """
        returns a log file name
        :return: name of the log file in s3
        """
        filename = "{}_{}".format(self.key, self.start_time)
        if not self.compress:
            return filename
        return "{}.gz".format(filename)

    def _add_task(self, task):
        self._rotation_queue.put(task)

    def _join_tasks(self):
        self._rotation_queue.join()

    def _get_stream_object(self, filename):
        try:
            return StreamObject(self.s3, self.bucket.name, filename, self._stream_buffer_queue)

        except Exception:
            raise RuntimeError('Failed to open new S3 stream object')

    def _rotate_chunk(self, run_async=True):

        assert self._current_object, "Stream object not found"

        part_num = self._current_object.chunk_count + 1
        part = self._current_object.uploader.Part(part_num)
        buffer = self._current_object.buffer
        self._current_object.buffer = BytesIO()
        buffer.seek(0)
        if run_async:
            self._current_object.add_task(Task(self._upload_part, self._current_object, part, part_num, buffer))
        else:
            self._upload_part(self._current_object, part, part_num, buffer)

        self._current_object.chunk_count += 1

    @staticmethod
    def _upload_part(s3_object, part, part_num, buffer):
        upload = part.upload(Body=buffer)
        s3_object.parts.append({'ETag': upload['ETag'], 'PartNumber': part_num})

    def _rotate_file(self):

        if self._current_object.buffer.tell() > 0:
            self._rotate_chunk()

        temp_object = self._current_object
        self._add_task(Task(self._close_stream, stream_object=temp_object))
        self.start_time = int(datetime.utcnow().strftime('%s'))
        new_filename = self.get_filename()
        self._current_object = self._get_stream_object(new_filename)

    @staticmethod
    def _close_stream(stream_object, callback=None, *args, **kwargs):
        stream_object.join_tasks()
        if stream_object.chunk_count > 0:
            stream_object.uploader.complete(MultipartUpload={'Parts': stream_object.parts})
        else:
            stream_object.uploader.abort()

        if callback and callable(callback):
            callback(*args, **kwargs)

    def close(self, *args, **kwargs):
        """
        close the stream for writing, upload remaining log records in stream
        :param args:
        :param kwargs:
        :return:
        """

        if self._current_object.buffer.tell() > 0:
            self._rotate_chunk(run_async=False)

        self._current_object.join_tasks()
        self._join_tasks()

        self._close_stream(self._current_object)

        # Stop the worker threads
        for _ in range(len(self.workers)):
            self._rotation_queue.put(STOP_SIGNAL)

        for _ in range(len(self._stream_bg_workers)):
            self._stream_buffer_queue.put(STOP_SIGNAL)

        self._is_open = False

    @property
    def closed(self):
        return not self._is_open

    @property
    def writable(self, *args, **kwargs):
        return True

    def tell(self, *args, **kwargs):
        """
        indication of current size of the stream before rotation
        :param args:
        :param kwargs:
        :return: size of the current stream
        """
        return self._current_object.byte_count

    def write(self, *args, **kwargs):
        """
        writes a log record to the stream
        :param args:
        :param kwargs:
        :return: size of record that was written
        """
        s = self.compress and gzip.compress(args[0].encode(self.encoder)) or args[0].encode(self.encoder)
        self._current_object.buffer.write(s)
        self._current_object.byte_count = self._current_object.byte_count + len(s)
        return len(s)

    def flush(self, *args, **kwargs):
        """
        flushes the current stream if it exceeds the threshold size
        :return:
        """
        if self._current_object.buffer.tell() > self.chunk_size:
            self._rotate_chunk()

        if (self.max_file_size_bytes and self._current_object.byte_count > self.max_file_size_bytes) or (
                self.max_file_log_time and int(
            datetime.utcnow().strftime('%s')) - self.start_time > self.max_file_log_time):
            self._rotate_file()


class S3Handler(StreamHandler):
    """
    A Logging handler class that streams log records to S3 by chunks
    """

    def __init__(self, key: str, bucket: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE,
                 time_rotation: int = DEFAULT_ROTATION_TIME_SECS, max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
                 encoder: str = 'utf-8',
                 max_threads: int = 1, compress: bool = False, **boto_session_kwargs):
        """
        :param key: The path of the S3 object
        :type key: str
        :param bucket: The id of the S3 bucket
        :type bucket: str
        :param chunk_size: size of a chunk in the multipart upload in bytes (default 5MB)
        :type chunk_size: int
        :param time_rotation: Interval in seconds to rotate the file by (default 12 hours)
        :type time_rotation: int
        :param max_file_size_bytes: maximum file size in bytes before rotation (default 100MB)
        :type max_file_size_bytes: int
        :param encoder: default utf-8
        :type encoder: str
        :param max_threads: the number of threads that a stream handler would run for file and chunk rotation tasks,
               only useful if emitting lot's of records
        :type max_threads: int
        :param compress: indicating weather to save a compressed gz suffixed file
        :type compress: bool
        """

        args_validation = (
            ValidationRule(time_rotation, is_positive_int, bad_integer_err('time_rotation')),
            ValidationRule(max_file_size_bytes, is_positive_int, bad_integer_err('max_file_size_bytes')),
            ValidationRule(encoder, is_non_empty_string, empty_str_err('encoder')),
            ValidationRule(max_threads, is_positive_int, bad_integer_err('thread_count')),
        )

        for rule in args_validation:
            assert rule.func(rule.arg), rule.message

        self.bucket = bucket
        try:
            self.stream = S3Stream(self.bucket, key, chunk_size=chunk_size, max_file_log_time=time_rotation,
                                   max_file_size_bytes=max_file_size_bytes, encoder=encoder, workers=max_threads,
                                   compress=compress, **boto_session_kwargs)
        except ConnectionError:
            logger.error("Unable to open S3 Stream. Telemetry will be disabled.")
            self.stream = None  # Will therefore use stderr via call to parent's constructor
            self.emit = self._no_op_emit

        # Make sure we gracefully clear the buffers and upload the missing parts before exiting
        self._sigterm_handler = signal.signal(signal.SIGTERM, self._teardown)
        self._sigint_handler = signal.signal(signal.SIGINT, self._teardown)
        self._sigquit_handler = signal.signal(signal.SIGQUIT, self._teardown)
        atexit.register(self.close)

        super(S3Handler, self).__init__(self.stream)

    def emit(self, record):
        try:
            super().emit(record)
        except ConnectionError:
            # Silently allow emitted records not to do anything.
            pass

    def _no_op_emit(self, record):
        # This method simply does nothing; it can be used to patch our emit method if our stream is not writable
        pass

    def _teardown(self, signum: int, frame):
        self.close()
        if signum == signal.SIGTERM:
            self._sigterm_handler(signum, frame)
        elif signum == signal.SIGINT:
            self._sigint_handler(signum, frame)
        elif signum == signal.SIGQUIT:
            self._sigquit_handler(signum, frame)

    def close(self, *args, **kwargs):
        """
        Closes the stream
        """
        self.acquire()
        try:
            if self.stream:
                try:
                    self.flush()
                finally:
                    stream = self.stream
                    self.stream = None
                    if hasattr(stream, "close"):
                        stream.close(*args, **kwargs)
        finally:
            self.release()
