def get_amqp_errors():
    # We want to define a list of all amqp errors we should handle in order
    # to parametrize them in tests. However, we don't want to block
    # the test suite from running in case a user hasn't installed Pika.
    try:
        from pika.exceptions import (
            AMQPChannelError,
            AMQPConnectionError,
            AMQPError,
            AuthenticationError,
            ChannelClosed,
            ChannelClosedByBroker,
            ChannelClosedByClient,
            ChannelError,
            ChannelWrongStateError,
            ConnectionBlockedTimeout,
            ConnectionClosed,
            ConnectionClosedByBroker,
            ConnectionClosedByClient,
            ConnectionOpenAborted,
            ConnectionWrongStateError,
            ProbableAccessDeniedError,
            ProbableAuthenticationError,
            StreamLostError,
        )

        errors = [
            AMQPError,
            AMQPChannelError,
            AMQPConnectionError,
            ChannelClosed(reply_code=0, reply_text=""),
            AuthenticationError,
            ChannelClosedByBroker(reply_code=0, reply_text=""),
            ChannelClosedByClient(reply_code=0, reply_text=""),
            ChannelError(),
            ChannelWrongStateError,
            ConnectionBlockedTimeout,
            ConnectionClosed(reply_code=0, reply_text=""),
            ConnectionClosedByBroker(reply_code=0, reply_text=""),
            ConnectionClosedByClient(reply_code=0, reply_text=""),
            ConnectionOpenAborted,
            ConnectionWrongStateError,
            ProbableAccessDeniedError,
            ProbableAuthenticationError,
            StreamLostError,
        ]
    except ImportError:
        errors = []
    return errors
