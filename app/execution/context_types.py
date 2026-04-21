from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class ContextLevel(str, Enum):
    SYSTEM = "system"
    OPERATIONAL = "operational"
    SESSION = "session"


class ContextMutability(str, Enum):
    IMMUTABLE = "immutable"
    CONTROLLED = "controlled"
    EPHEMERAL = "ephemeral"


class ArchitectureSeparation(str, Enum):
    STRICT = "strict"


class ProductRuntimeScope(str, Enum):
    BUSINESS_ONLY = "business_only"


class ProductRole(str, Enum):
    BUSINESS_OPERATIONS = "business_operations"


class PrimaryChannel(str, Enum):
    VOICE = "voice"
    SMS = "sms"
    EMAIL = "email"


class LayerDescriptor(BaseModel):
    level: ContextLevel
    mutability: ContextMutability


class ContextMetadata(BaseModel):
    revision: int = Field(default=1, ge=1)
    system_layer: LayerDescriptor = Field(
        default_factory=lambda: LayerDescriptor(
            level=ContextLevel.SYSTEM,
            mutability=ContextMutability.IMMUTABLE,
        )
    )
    operational_layer: LayerDescriptor = Field(
        default_factory=lambda: LayerDescriptor(
            level=ContextLevel.OPERATIONAL,
            mutability=ContextMutability.CONTROLLED,
        )
    )
    session_layer: LayerDescriptor = Field(
        default_factory=lambda: LayerDescriptor(
            level=ContextLevel.SESSION,
            mutability=ContextMutability.EPHEMERAL,
        )
    )
    precedence: tuple[ContextLevel, ...] = (
        ContextLevel.SYSTEM,
        ContextLevel.OPERATIONAL,
        ContextLevel.SESSION,
    )


class SystemSettings(BaseModel):
    dev_environment: str
    product_environment: str
    architecture_separation: ArchitectureSeparation


class ProductBoxSettings(BaseModel):
    runtime_scope: ProductRuntimeScope = ProductRuntimeScope.BUSINESS_ONLY
    company_name: str
    description: str = ""
    is_dev_environment: bool = False
    code_generation_allowed: bool = False
    roles: tuple[ProductRole, ...]


class InteractionSettings(BaseModel):
    primary_channels: tuple[PrimaryChannel, ...]


class OperationalSettings(BaseModel):
    owner_role: str
    company_name: str


class ContextSnapshot(BaseModel):
    context: ContextMetadata = Field(default_factory=ContextMetadata)
    system: SystemSettings
    product_box: ProductBoxSettings
    interaction: InteractionSettings
    operational: OperationalSettings
