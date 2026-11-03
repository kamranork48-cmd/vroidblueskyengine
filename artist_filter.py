from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArtistAssessment:
    artist_score: int
    audience_score: int
    commission_artist: bool
    signals: tuple[str, ...]


ARTIST_SIGNALS = (
    ("commissions_open", ("commissions open", "commission slots", "taking commissions", "artist for hire"), 5),
    ("commission_service", ("art commissions", "commission info", "commission prices", "ych", "adoptables"), 4),
    ("professional_artist", ("digital artist", "illustrator", "concept artist", "freelance artist", "art studio"), 2),
    ("paid_service", ("ko-fi", "kofi", "vgen", "portfolio", "patreon"), 1),
)

AUDIENCE_SIGNALS = (
    ("streamer", ("twitch", "streamer", "streaming", "youtube gaming"), 5),
    ("vtuber", ("vtuber", "vtubing", "pngtuber", "envtuber"), 5),
    ("gamer", ("gamer", "gaming", "video games", "game dev"), 3),
    ("furry", ("furry", "fursona", "anthro", "scalie"), 3),
    ("anime", ("anime", "manga", "cosplay", "otaku"), 2),
    ("nsfw_interest", ("nsfw", "18+", "adults only"), 2),
)


def assess_profile(
    text: str,
    *,
    artist_keywords: tuple[str, ...] = (),
    audience_keywords: tuple[str, ...] = (),
) -> ArtistAssessment:
    normalized = text.casefold()
    artist_score = 0
    audience_score = 0
    signals: list[str] = []
    for name, phrases, weight in ARTIST_SIGNALS:
        if any(phrase in normalized for phrase in phrases):
            artist_score += weight
            signals.append(name)
    for name, phrases, weight in AUDIENCE_SIGNALS:
        if any(phrase in normalized for phrase in phrases):
            audience_score += weight
            signals.append(name)
    for keyword in artist_keywords:
        if keyword.casefold() in normalized:
            artist_score += 2
            signals.append(f"custom_artist:{keyword}")
    for keyword in audience_keywords:
        if keyword.casefold() in normalized:
            audience_score += 2
            signals.append(f"custom_audience:{keyword}")
    commission_artist = artist_score >= 5 and any(
        signal in signals for signal in ("commissions_open", "commission_service")
    )
    return ArtistAssessment(artist_score, audience_score, commission_artist, tuple(signals))
