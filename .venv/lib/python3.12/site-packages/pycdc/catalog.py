"""Built-in gift catalog with curated ideas by occasion and budget."""

from __future__ import annotations

from .models import GiftIdea, Occasion, Budget

# Curated gift catalog
CATALOG: list[GiftIdea] = [
    # Token gifts
    GiftIdea("Handwritten Thank You Card", "Premium card with personal note", "$5-10",
             Budget.TOKEN, Occasion.THANK_YOU, category="stationery",
             personalization_tip="Reference a specific project or win you shared"),
    GiftIdea("Custom Sticker Pack", "Branded fun stickers for their laptop", "$8-12",
             Budget.TOKEN, Occasion.ONBOARDING, category="swag",
             personalization_tip="Include their company logo alongside yours"),
    GiftIdea("Artisan Coffee Sampler", "4-pack of single-origin coffee", "$12-15",
             Budget.TOKEN, Occasion.REFERRAL, category="food",
             personalization_tip="Add a note: 'Thanks a latte for the referral'"),

    # Modest gifts
    GiftIdea("Desk Plant Kit", "Low-maintenance succulent in branded pot", "$20-35",
             Budget.MODEST, Occasion.ONBOARDING, category="office",
             personalization_tip="Choose a plant that matches their office aesthetic"),
    GiftIdea("Book — Their Industry", "Bestseller relevant to their field", "$15-30",
             Budget.MODEST, Occasion.MILESTONE, category="books",
             personalization_tip="Write a note inside the cover about why you picked it"),
    GiftIdea("Gourmet Cookie Box", "Assorted artisan cookies, 12-pack", "$25-40",
             Budget.MODEST, Occasion.HOLIDAY, category="food",
             personalization_tip="Check for dietary restrictions first"),
    GiftIdea("Charity Donation in Their Name", "Donation to a cause they care about", "$25-50",
             Budget.MODEST, Occasion.THANK_YOU, category="charity",
             personalization_tip="Pick a cause related to their stated values or interests"),

    # Standard gifts
    GiftIdea("Premium Bluetooth Speaker", "Compact high-quality speaker", "$60-100",
             Budget.STANDARD, Occasion.CLOSING_DEAL, category="tech",
             personalization_tip="Engrave their initials or a short message"),
    GiftIdea("Wine & Cheese Hamper", "Curated selection with tasting notes", "$75-120",
             Budget.STANDARD, Occasion.ANNIVERSARY, category="food",
             personalization_tip="Include wines from their home region if possible"),
    GiftIdea("Spa Gift Card", "Relaxation package at a local spa", "$80-150",
             Budget.STANDARD, Occasion.RENEWAL, category="experience",
             personalization_tip="'Celebrating another year of partnership — you deserve a break'"),
    GiftIdea("Custom Illustration", "Commissioned portrait or office artwork", "$50-120",
             Budget.STANDARD, Occasion.MILESTONE, category="art",
             personalization_tip="Commission art of their office building or team mascot"),

    # Premium gifts
    GiftIdea("Weekend Getaway Voucher", "Hotel voucher for a weekend escape", "$200-400",
             Budget.PREMIUM, Occasion.CLOSING_DEAL, category="experience",
             personalization_tip="Pick a destination near their city or a place they've mentioned"),
    GiftIdea("Noise-Cancelling Headphones", "Sony or Bose premium headphones", "$250-350",
             Budget.PREMIUM, Occasion.RENEWAL, category="tech",
             personalization_tip="Great for clients who travel frequently"),
    GiftIdea("Leather Portfolio Set", "Handcrafted leather notebook + pen", "$150-300",
             Budget.PREMIUM, Occasion.PROMOTION, category="luxury",
             personalization_tip="Monogram with their initials"),

    # VIP gifts
    GiftIdea("Private Dining Experience", "Chef's table dinner for two", "$500-800",
             Budget.VIP, Occasion.ANNIVERSARY, category="experience",
             personalization_tip="Invite them personally — make it a relationship-building event"),
    GiftIdea("Custom Watch Engraving", "Premium watch with personalized engraving", "$500-1500",
             Budget.VIP, Occasion.MILESTONE, category="luxury",
             personalization_tip="Engrave the date of your first deal together"),
    GiftIdea("Team Offsite Sponsorship", "Fund a team activity for their company", "$1000+",
             Budget.VIP, Occasion.RENEWAL, category="experience",
             personalization_tip="Offer to cover an escape room, cooking class, or wine tasting"),
]


def search_catalog(*, occasion: Occasion | None = None, budget: Budget | None = None,
                   query: str = "", interests: list[str] | None = None) -> list[GiftIdea]:
    """Search the built-in catalog."""
    results = list(CATALOG)

    if occasion:
        results = [g for g in results if g.occasion == occasion]
    if budget:
        results = [g for g in results if g.budget == budget]
    if query:
        q = query.lower()
        results = [g for g in results if q in g.name.lower() or q in g.description.lower()
                   or q in g.category.lower()]
    if interests:
        interest_set = {i.lower() for i in interests}
        scored = []
        for g in results:
            text = f"{g.name} {g.description} {g.category}".lower()
            matches = sum(1 for i in interest_set if i in text)
            g.score = matches / len(interest_set) if interest_set else 0
            scored.append(g)
        results = sorted(scored, key=lambda g: g.score, reverse=True)

    return results
