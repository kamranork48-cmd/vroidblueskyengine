import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ArtistAssessment:
    artist_score: int
    audience_score: int
    excluded_provider: bool
    signals: tuple[str, ...]


COMMISSION_OPEN_PHRASES = (
    "commissions open",
    "commission open",
    "commision open",
    "commisions open",
    "comission open",
    "comissions open",
    "comms open",
    "comm open",
    "artist for hire",
    "taking commissions",
    "taking comms",
    "open for commissions",
)

COMMISSION_SERVICE_PHRASES = (
    "art commissions",
    "artist commissions",
    "artist commission",
    "artist commisions",
    "artist commision",
    "artist comissions",
    "artist comission",
    "commission info",
    "commission prices",
    "commission sheet",
    "commision info",
    "commision prices",
    "commision sheet",
    "comission info",
    "comission prices",
    "comission sheet",
    "my commissions",
    "ych",
    "adoptables",
)

ARTIST_IDENTITY_PHRASES = (
    "artist",
    "artists",
    "furry artist",
    "digital artist",
    "graphic artist",
    "gfx artist",
    "illustrator",
    "illustrators",
    "concept artist",
    "character artist",
    "designer",
    "designers",
    "graphic designer",
    "gfx designer",
    "character designer",
    "vtuber artist",
    "live2d artist",
    "live2d rigger",
    "art director",
    "freelance artist",
    "art studio",
)

SELF_IDENTIFICATION_PHRASES = (
    "i am an artist",
    "i'm an artist",
    "im an artist",
    "i am a designer",
    "i'm a designer",
    "im a designer",
    "i am an illustrator",
    "i'm an illustrator",
    "im an illustrator",
    "i take commissions",
    "i do commissions",
)

SEEKING_CREATIVE_PHRASES = (
    "looking for an artist",
    "looking for artists",
    "looking for a graphic designer",
    "looking for a designer",
    "looking for an illustrator",
    "need an artist",
    "need a graphic designer",
    "need a designer",
    "want to hire an artist",
    "want to hire a designer",
    "seeking an artist",
    "seeking a designer",
    "commissioning an artist",
)

AUDIENCE_SIGNALS = (
    ("streamer", ("twitch", "streamer", "streaming", "youtube gaming"), 5),
    ("vtuber", ("vtuber", "vtubing", "pngtuber", "envtuber"), 5),
    ("gamer", ("gamer", "gaming", "video games", "game dev"), 3),
    ("furry", ("furry", "fursona", "anthro", "scalie"), 3),
    ("anime", ("anime", "manga", "cosplay", "otaku"), 2),
    ("nsfw_interest", ("nsfw", "18+", "adults only"), 2),
)


def _normalize(text: str) -> str:
    return re.sub(r"[^\w]+", " ", text.casefold(), flags=re.UNICODE).strip()


def _contains_phrase(normalized_text: str, phrase: str) -> bool:
    normalized_phrase = _normalize(phrase)
    return bool(
        normalized_phrase
        and re.search(rf"(?<!\w){re.escape(normalized_phrase)}(?!\w)", normalized_text)
    )


def assess_profile(
    text: str,
    *,
    profile_text: str | None = None,
    artist_keywords: tuple[str, ...] = (),
    audience_keywords: tuple[str, ...] = (),
) -> ArtistAssessment:
    normalized = _normalize(text)
    normalized_profile = _normalize(profile_text if profile_text is not None else text)
    artist_score = 0
    audience_score = 0
    signals: list[str] = []

    for name, phrases, weight in (
        ("commissions_open", COMMISSION_OPEN_PHRASES, 5),
        ("commission_service", COMMISSION_SERVICE_PHRASES, 4),
    ):
        if any(_contains_phrase(normalized, phrase) for phrase in phrases):
            artist_score += weight
            signals.append(name)

    seeking_creative = any(
        _contains_phrase(normalized_profile, phrase) for phrase in SEEKING_CREATIVE_PHRASES
    )
    if not seeking_creative and any(
        _contains_phrase(normalized_profile, phrase) for phrase in ARTIST_IDENTITY_PHRASES
    ):
        artist_score += 4
        signals.append("professional_artist")
    elif any(_contains_phrase(normalized, phrase) for phrase in SELF_IDENTIFICATION_PHRASES):
        artist_score += 4
        signals.append("professional_artist")

    for keyword in artist_keywords:
        if _contains_phrase(normalized, keyword):
            artist_score += 2
            signals.append(f"custom_artist:{keyword}")
    for name, phrases, weight in AUDIENCE_SIGNALS:
        if any(_contains_phrase(normalized, phrase) for phrase in phrases):
            audience_score += weight
            signals.append(name)
    for keyword in audience_keywords:
        if _contains_phrase(normalized, keyword):
            audience_score += 2
            signals.append(f"custom_audience:{keyword}")

    excluded_provider = any(
        signal in signals
        for signal in ("commissions_open", "commission_service", "professional_artist")
    ) or any(signal.startswith("custom_artist:") for signal in signals)
    return ArtistAssessment(artist_score, audience_score, excluded_provider, tuple(signals))
