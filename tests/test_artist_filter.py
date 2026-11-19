import asyncio
import unittest

from artist_filter import assess_profile
from bsky_client import Profile
from config import Settings
from worker import BotWorker


class RecentPostsClient:
    def __init__(self, recent_posts: str = "") -> None:
        self.recent_posts = recent_posts

    def recent_post_text(self, actor: str, limit: int) -> str:
        return self.recent_posts


class ArtistFilterTests(unittest.TestCase):
    def test_artist_and_designer_identities_are_excluded(self) -> None:
        for profile_text in (
            "Bisexual Artist",
            "Graphic Designer",
            "GFX Artist",
            "Furry Artist",
        ):
            with self.subTest(profile_text=profile_text):
                assessment = assess_profile(profile_text, profile_text=profile_text)
                self.assertTrue(assessment.excluded_provider)

    def test_commission_open_variants_are_excluded(self) -> None:
        for phrase in (
            "comms open",
            "commission open",
            "commision open",
            "artist commissions open",
            "artist commision",
        ):
            with self.subTest(phrase=phrase):
                assessment = assess_profile(phrase, profile_text="VTuber")
                self.assertTrue(assessment.excluded_provider)

    def test_ordinary_audience_profile_is_not_excluded(self) -> None:
        assessment = assess_profile(
            "VTuber looking for an artist for my debut",
            profile_text="VTuber looking for an artist for my debut",
        )
        self.assertFalse(assessment.excluded_provider)

    def test_worker_rejects_artist_before_following(self) -> None:
        worker = BotWorker(
            Settings(artist_filter_enabled=True, exclude_commission_artists=True),
            None,
            RecentPostsClient("I like NSFW 3D models."),
            lambda message: None,
        )
        profile = Profile(
            handle="sidiinsidious.bsky.social",
            did="did:plc:example",
            display_name="Sidi After Hours",
            bio="Bisexual Artist",
            followers_count=25,
        )

        matched = asyncio.run(worker._matches(profile, "nsfw 3d model"))

        self.assertFalse(matched)


if __name__ == "__main__":
    unittest.main()
