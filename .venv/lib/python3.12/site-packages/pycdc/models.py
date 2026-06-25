"""Core models for customer profiles, occasions, and gift ideas."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Occasion(str, Enum):
    ONBOARDING = "onboarding"
    RENEWAL = "renewal"
    BIRTHDAY = "birthday"
    HOLIDAY = "holiday"
    MILESTONE = "milestone"
    THANK_YOU = "thank_you"
    APOLOGY = "apology"
    REFERRAL = "referral"
    ANNIVERSARY = "anniversary"
    CLOSING_DEAL = "closing_deal"
    PROMOTION = "promotion"
    CUSTOM = "custom"


class Budget(str, Enum):
    TOKEN = "token"           # $0-15
    MODEST = "modest"         # $15-50
    STANDARD = "standard"     # $50-150
    PREMIUM = "premium"       # $150-500
    VIP = "vip"               # $500+

    @property
    def range(self) -> tuple[int, int]:
        return {
            "token": (0, 15),
            "modest": (15, 50),
            "standard": (50, 150),
            "premium": (150, 500),
            "vip": (500, 10000),
        }[self.value]


@dataclass
class CustomerProfile:
    """Profile of a customer for personalized gift recommendations."""
    name: str
    company: str = ""
    role: str = ""
    industry: str = ""
    interests: list[str] = field(default_factory=list)
    location: str = ""
    relationship_length_months: int = 0
    deal_value: float = 0.0
    notes: str = ""

    @property
    def tier(self) -> str:
        if self.deal_value >= 100_000:
            return "enterprise"
        elif self.deal_value >= 10_000:
            return "business"
        elif self.deal_value >= 1_000:
            return "professional"
        return "starter"


@dataclass
class GiftIdea:
    """A single gift recommendation."""
    name: str
    description: str
    price_range: str
    budget: Budget
    occasion: Occasion
    source_url: str = ""
    category: str = ""
    personalization_tip: str = ""
    score: float = 0.0  # relevance score 0-1

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "price_range": self.price_range,
            "budget": self.budget.value,
            "occasion": self.occasion.value,
            "source_url": self.source_url,
            "category": self.category,
            "personalization_tip": self.personalization_tip,
            "score": self.score,
        }
