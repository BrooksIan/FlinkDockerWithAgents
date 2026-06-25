"""GiftFinder — main client that combines catalog search with web search."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from .models import GiftIdea, Occasion, Budget, CustomerProfile
from .catalog import search_catalog, CATALOG


@dataclass
class GiftFinder:
    """Search for CRM gift ideas from built-in catalog and the web.

    Parameters
    ----------
    enable_web : bool
        Whether to also search the web for ideas (default True).
    """

    enable_web: bool = True

    def find(self, *, occasion: Occasion | None = None, budget: Budget | None = None,
             query: str = "", customer: CustomerProfile | None = None) -> list[GiftIdea]:
        """Find gift ideas matching criteria.

        Searches the built-in catalog and optionally the web.
        """
        interests = customer.interests if customer else None

        # Catalog results
        results = search_catalog(occasion=occasion, budget=budget,
                                 query=query, interests=interests)

        # Boost score for customer tier match
        if customer and budget is None:
            tier_budget = {
                "enterprise": Budget.VIP,
                "business": Budget.PREMIUM,
                "professional": Budget.STANDARD,
                "starter": Budget.MODEST,
            }
            preferred = tier_budget.get(customer.tier, Budget.STANDARD)
            for g in results:
                if g.budget == preferred:
                    g.score += 0.3

        # Web search (if enabled)
        if self.enable_web and (query or (customer and customer.interests)):
            web_results = asyncio.run(self._web_search(
                query=query, occasion=occasion, budget=budget, customer=customer))
            results.extend(web_results)

        # Sort by score descending
        results.sort(key=lambda g: g.score, reverse=True)
        return results

    def recommend(self, customer: CustomerProfile, occasion: Occasion) -> list[GiftIdea]:
        """Smart recommendations based on customer profile and occasion."""
        # Determine budget from customer tier
        tier_budget = {
            "enterprise": Budget.VIP,
            "business": Budget.PREMIUM,
            "professional": Budget.STANDARD,
            "starter": Budget.MODEST,
        }
        budget = tier_budget.get(customer.tier, Budget.STANDARD)

        ideas = self.find(occasion=occasion, budget=budget, customer=customer)

        # Further personalize tips
        for idea in ideas:
            if customer.name and idea.personalization_tip:
                idea.personalization_tip = (
                    f"For {customer.name}: {idea.personalization_tip}"
                )

        return ideas[:5]

    async def _web_search(self, *, query: str, occasion: Occasion | None,
                          budget: Budget | None, customer: CustomerProfile | None) -> list[GiftIdea]:
        """Search the web for gift ideas (uses DuckDuckGo instant answers)."""
        search_terms = []
        if query:
            search_terms.append(query)
        if occasion:
            search_terms.append(f"{occasion.value} gift")
        if budget:
            low, high = budget.range
            search_terms.append(f"${low}-${high}")
        if customer and customer.interests:
            search_terms.extend(customer.interests[:2])
        search_terms.append("corporate gift idea")

        search_query = " ".join(search_terms)

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    "https://api.duckduckgo.com/",
                    params={"q": search_query, "format": "json", "no_html": 1},
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()

                results = []
                for topic in data.get("RelatedTopics", [])[:5]:
                    text = topic.get("Text", "")
                    url = topic.get("FirstURL", "")
                    if text:
                        results.append(GiftIdea(
                            name=text[:60],
                            description=text,
                            price_range="varies",
                            budget=budget or Budget.STANDARD,
                            occasion=occasion or Occasion.CUSTOM,
                            source_url=url,
                            category="web-suggestion",
                            score=0.3,
                        ))
                return results
            except Exception:
                return []

    def summary_for(self, customer: CustomerProfile) -> str:
        """Print a customer gift strategy summary."""
        lines = [
            f"Gift Strategy for {customer.name}",
            f"  Company: {customer.company} ({customer.industry})",
            f"  Tier: {customer.tier} (deal value: ${customer.deal_value:,.0f})",
            f"  Interests: {', '.join(customer.interests) or 'unknown'}",
            f"  Relationship: {customer.relationship_length_months} months",
            "",
        ]

        for occasion in [Occasion.RENEWAL, Occasion.HOLIDAY, Occasion.THANK_YOU]:
            recs = self.recommend(customer, occasion)
            if recs:
                lines.append(f"  [{occasion.value.upper()}]")
                for r in recs[:2]:
                    lines.append(f"    • {r.name} ({r.price_range}) — {r.personalization_tip}")
                lines.append("")

        return "\n".join(lines)
