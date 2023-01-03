import logging

from great_expectations.metric_computations.util import pluralize, underscore

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy.ext.declarative import as_declarative, declared_attr
    from sqlalchemy.orm import backref, relationship, validates
except ImportError:
    logger.debug("No SqlAlchemy module available.")
    sa = None
    backref = None
    relationship = None
    validates = None
    as_declarative = None
    declared_attr = None


# TODO: <Alex>ALEX</Alex>
# from sqlalchemy.dialects import postgresql
# from sqlalchemy.dialects.postgresql import UUID
# TODO: <Alex>ALEX</Alex>

metadata = sa.MetaData()


@as_declarative(metadata=metadata)
class Base:
    """
    Common attributes that can be re-used by other SQLAlchemy models
    """

    # noinspection PyMethodParameters
    @declared_attr
    def __tablename__(cls):
        return pluralize(underscore(cls.__name__))

    # TODO: <Alex>ALEX</Alex>
    # @declared_attr
    # def query(cls):
    #     query = db_session.query_property()
    #     return query
    #
    # @declared_attr
    # def session(cls):
    #     return db_session
    # TODO: <Alex>ALEX</Alex>


class PrimaryKeyMixin:
    id = (
        sa.Column(
            sa.Integer(),
            nullable=False,
            primary_key=True,
        ),
    )


class TimestampsMixin:
    created_at = (
        sa.Column(
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    updated_at = (
        sa.Column(
            sa.DateTime(),
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )

    @validates("created_at")
    def _write_once(self, key, value):
        existing = getattr(self, key, None)
        if existing is not None:
            raise ValueError("Field '%s' is write-once" % key)
        return value


class SoftDeleteMixin:
    deleted_at = sa.Column(
        sa.DateTime(),
        nullable=True,
        # onupdate=get_onupdate_timestamp_callback(attribute_name="deleted"),
    )
    deleted = (
        sa.Column(
            sa.Boolean(),
            nullable=False,
            default=False,
        ),
    )
    # TODO: <Alex>ALEX</Alex>
    # status = sa.Column(
    #     sa.Integer(),
    #     nullable=False,
    #     default=0,
    # ),
    # TODO: <Alex>ALEX</Alex>


class ArchiveMixin:
    archived_at = sa.Column(
        sa.DateTime(),
        nullable=True,
    )
    archived = (
        sa.Column(
            sa.Boolean(),
            nullable=False,
            default=False,
        ),
    )

    # TODO: <Alex>ALEX</Alex>
    # @declared_attr
    # def archived_by_id(cls):
    #     return sa.Column(UUID(as_uuid=True), sa.ForeignKey("users.id"))
    # TODO: <Alex>ALEX</Alex>


class AccountMixin:
    data_context_uuid = sa.Column(
        sa.UnicodeText(),
        nullable=True,
    )
