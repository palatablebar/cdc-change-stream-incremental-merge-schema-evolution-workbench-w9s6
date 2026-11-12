"""后端枚举定义（前端 frontend/src/constants/*.ts 是第二份，靠 README 对照表同步）。"""
from __future__ import annotations

from enum import Enum


class CdcOp(str, Enum):
    """binlog 操作类型。delete 后再 insert 视为重建。"""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


class StreamStatus(str, Enum):
    HEALTHY = "healthy"
    LAGGING = "lagging"
    PAUSED = "paused"
    ERROR = "error"

    @classmethod
    def values(cls) -> set[str]:
        return {member.value for member in cls}


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"

    @classmethod
    def terminal(cls) -> set[str]:
        return {cls.SUCCEEDED.value, cls.FAILED.value, cls.CANCELED.value}


class CatchupStatus(str, Enum):
    """连续增量追赶会话状态。

    active   = 有批次在排队/归并中（链上自动推进）
    idle     = 上一批已确认落账，等待手动/重试投下一批（可放心暂停的安全点）
    blocked  = 水位线紧邻位置出现 LSN 空洞，停在空洞之前，等补齐
    paused   = 值班同学主动暂停（当前批次跑完后不再自动续）
    completed= 已追到最新位点，无剩余积压
    failed   = 最近一批失败且未恢复（确认位点未动，可一键续跑）
    """

    ACTIVE = "active"
    IDLE = "idle"
    BLOCKED = "blocked"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def terminal(cls) -> set[str]:
        return {cls.COMPLETED.value}

    @classmethod
    def open_values(cls) -> set[str]:
        """尚未终结、仍可继续推进的状态。"""
        return {
            cls.ACTIVE.value,
            cls.IDLE.value,
            cls.BLOCKED.value,
            cls.PAUSED.value,
            cls.FAILED.value,
        }


class RejectReason(str, Enum):
    """拒绝事件原因枚举，决定重放修复策略。"""

    SCHEMA_MISMATCH = "schema_mismatch"
    PK_MISSING = "pk_missing"
    PARSE_ERROR = "parse_error"
    TYPE_CONFLICT = "type_conflict"


class RejectStatus(str, Enum):
    PENDING = "pending"
    REPLAYED = "replayed"
    DISMISSED = "dismissed"


class MergeStrategy(str, Enum):
    UPSERT = "upsert"
    APPEND = "append"


class ChangeKind(str, Enum):
    """schema 字段级变更类别（算法 2 的输入分类）。"""

    ADD_NULLABLE = "add_nullable"
    ADD_REQUIRED = "add_required"
    ADD_REQUIRED_WITH_DEFAULT = "add_required_with_default"
    DROP_COLUMN = "drop_column"
    TYPE_WIDEN = "type_widen"
    TYPE_NARROW = "type_narrow"
    TYPE_INCOMPATIBLE = "type_incompatible"
    NULLABILITY_TO_REQUIRED = "nullability_to_required"
    NULLABILITY_TO_NULLABLE = "nullability_to_nullable"
    PK_CHANGE = "pk_change"

    @classmethod
    def values(cls) -> set[str]:
        return {member.value for member in cls}


class CompatLevel(str, Enum):
    """兼容等级；汇总时取最差。"""

    FULL = "full"
    BACKWARD = "backward"
    FORWARD = "forward"
    BREAKING = "breaking"

    @classmethod
    def rank(cls, level: str) -> int:
        return {"full": 0, "backward": 1, "forward": 2, "breaking": 3}.get(level, 3)


class ActorKind(str, Enum):
    USER = "user"
    WORKER = "worker"


# 枚举中文展示名：后端 formatters / 接口文案共用
ENUM_LABELS: dict[str, dict[str, str]] = {
    "CdcOp": {"insert": "新增", "update": "更新", "delete": "删除"},
    "StreamStatus": {"healthy": "正常", "lagging": "延迟", "paused": "已暂停", "error": "异常"},
    "RunStatus": {
        "queued": "排队中",
        "running": "归并中",
        "succeeded": "成功",
        "failed": "失败",
        "canceled": "已取消",
    },
    "CatchupStatus": {
        "active": "追赶中",
        "idle": "待续跑",
        "blocked": "空洞阻塞",
        "paused": "已暂停",
        "completed": "已追平",
        "failed": "批次失败",
    },
    "RejectReason": {
        "schema_mismatch": "schema 不匹配",
        "pk_missing": "主键缺失",
        "parse_error": "载荷解析失败",
        "type_conflict": "类型冲突",
    },
    "RejectStatus": {"pending": "待处理", "replayed": "已重放", "dismissed": "已忽略"},
    "MergeStrategy": {"upsert": "按主键 upsert", "append": "仅追加"},
    "ChangeKind": {
        "add_nullable": "新增可空列",
        "add_required": "新增必填列（无默认值）",
        "add_required_with_default": "新增必填列（带默认值）",
        "drop_column": "删除列",
        "type_widen": "类型放宽",
        "type_narrow": "类型收窄",
        "type_incompatible": "类型不兼容",
        "nullability_to_required": "改为非空",
        "nullability_to_nullable": "改为可空",
        "pk_change": "主键变更",
    },
    "CompatLevel": {
        "full": "完全兼容",
        "backward": "向后兼容",
        "forward": "向前兼容",
        "breaking": "破坏性变更",
    },
}
