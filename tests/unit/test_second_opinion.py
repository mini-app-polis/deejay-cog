"""The second opinion on a file whose fingerprint and tags disagree: the
other AcoustID recordings first, then Spotify's length for each side."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from deejay_cog import second_opinion as so
from deejay_cog.second_opinion import Heard, Recording


@dataclass
class FakeSearch:
    """Spotify: the tracks it has, by the main artist and bare title asked."""

    tracks_by: dict[tuple[str, str], list[Recording]] = field(default_factory=dict)
    asked: list[tuple[str, str]] = field(default_factory=list)

    def tracks(self, artist: str, title: str) -> list[Recording]:
        key = so.search_terms(artist, title)
        self.asked.append(key)
        return self.tracks_by.get(key, [])


_BOOM_CLAP = ("Boom Clap", "Charli XCX")
_SUNNY = ("Sunny", "Boogie Pimps")


def test_acoustid_recordings_keeps_every_titled_recording_best_first() -> None:
    answer: dict[str, Any] = {
        "status": "ok",
        "results": [
            {
                "score": 0.6,
                "recordings": [{"id": "x", "title": "Low", "artists": [{"name": "A"}]}],
            },
            {
                "score": 0.98,
                "recordings": [
                    {
                        "id": "y",
                        "title": "Want to Want Me",
                        "duration": 207,
                        "artists": [
                            {"name": "Jason Derulo", "joinphrase": " feat. "},
                            {"name": "X"},
                        ],
                    },
                    {"id": "z"},  # no title: nothing to compare
                ],
            },
        ],
    }

    assert so.acoustid_recordings(answer) == (
        Recording("Want to Want Me", "Jason Derulo feat. X", 207.0),
        Recording("Low", "A"),
    )


def test_another_acoustid_recording_of_the_files_song_backs_its_tags() -> None:
    heard = Heard(
        207.0,
        (
            Recording("Want to Want Me (instrumental version)", "Jason Derulo"),
            Recording("Want to Want Me", "Jason Derulo"),
        ),
    )

    verdict = so.decide(
        "Want To Want Me",
        "Jason Derulo",
        "Want to Want Me (instrumental version)",
        "Jason Derulo",
        heard,
        None,
    )

    assert verdict.side == "tags"
    assert "Jason Derulo - Want to Want Me" in verdict.why


def test_spotify_backs_the_side_it_has_at_the_files_length() -> None:
    search = FakeSearch(
        {
            ("Charli XCX", "Boom Clap"): [Recording("Boom Clap", "Charli XCX", 169.6)],
            ("Boogie Pimps", "Sunny"): [Recording("Sunny", "Boogie Pimps", 412.0)],
        }
    )

    verdict = so.decide(*_BOOM_CLAP, *_SUNNY, Heard(169.0), search)

    assert verdict.side == "tags"
    assert verdict.why == (
        "Spotify has Charli XCX - Boom Clap at the file's length (2:49)"
    )


def test_spotify_can_back_the_fingerprint_over_the_tags() -> None:
    # The file is the remix of another artist's song, tagged as the remixer's.
    remix = Recording(
        "Overwhelmed - Ryan Mack Remix", "Royal & the Serpent, Ryan Mack", 103.0
    )
    search = FakeSearch(
        {
            ("Ryan Mack", "overwhelmed"): [remix],
            ("Royal", "Overwhelmed"): [remix],
        }
    )

    verdict = so.decide(
        "overwhelmed",
        "Ryan Mack",
        "Overwhelmed (Ryan Mack remix)",
        "Royal & the Serpent",
        Heard(103.4),
        search,
    )

    assert verdict.side == "fingerprint"


def test_a_track_at_another_length_or_version_backs_nothing() -> None:
    search = FakeSearch(
        {
            ("Charli XCX", "Boom Clap"): [
                Recording("Boom Clap", "Charli XCX", 190.0),  # another edit
                Recording("Boom Clap - Remix", "Charli XCX", 169.0),  # another version
            ],
        }
    )

    verdict = so.decide(*_BOOM_CLAP, *_SUNNY, Heard(169.0), search)

    assert verdict.side is None
    assert verdict.why == "Spotify has neither song at the file's length (2:49)"


def test_both_sides_backed_is_left_for_the_owner() -> None:
    search = FakeSearch(
        {
            ("Charli XCX", "Boom Clap"): [Recording("Boom Clap", "Charli XCX", 169.0)],
            ("Boogie Pimps", "Sunny"): [Recording("Sunny", "Boogie Pimps", 170.0)],
        }
    )

    verdict = so.decide(*_BOOM_CLAP, *_SUNNY, Heard(169.0), search)

    assert verdict.side is None and "both songs" in verdict.why


def test_without_spotify_or_a_length_there_is_no_second_opinion() -> None:
    assert so.decide(*_BOOM_CLAP, *_SUNNY, Heard(169.0), None) == so.Verdict(
        None, "no second opinion: Spotify is not available"
    )
    assert so.decide(*_BOOM_CLAP, *_SUNNY, None, FakeSearch()).side is None


def test_a_search_names_the_first_artist_and_the_bare_title() -> None:
    assert so.search_terms("Rihanna & Calvin Harris", "We Found Love") == (
        "Rihanna",
        "We Found Love",
    )
    assert so.search_terms("Vianney", "Pas là (Acoustic)") == ("Vianney", "Pas là")
    assert so.search_terms("Katja Krasavice x Leony", "Raindrops - Remix") == (
        "Katja Krasavice",
        "Raindrops",
    )


class _Spotipy:
    def __init__(self, answer: Any = None, fails: bool = False) -> None:
        self.answer, self.fails = answer, fails
        self.queries: list[str] = []

    def search(self, *, q: str, type: str, limit: int) -> Any:
        assert type == "track" and limit == so.SPOTIFY_RESULTS
        self.queries.append(q)
        if self.fails:
            raise RuntimeError("503")
        return self.answer


def test_spotify_search_reads_name_artists_and_length() -> None:
    client = _Spotipy(
        {
            "tracks": {
                "items": [
                    {
                        "name": "We Found Love",
                        "artists": [{"name": "Rihanna"}, {"name": "Calvin Harris"}],
                        "duration_ms": 215227,
                    }
                ]
            }
        }
    )

    found = so.SpotifyTrackSearch(client).tracks(
        "Rihanna & Calvin Harris", "We Found Love"
    )

    assert found == [Recording("We Found Love", "Rihanna, Calvin Harris", 215.227)]
    assert client.queries == ["artist:Rihanna & Calvin Harris track:We Found Love"]


def test_a_credit_that_finds_nothing_is_searched_by_its_first_artist() -> None:
    class _Empty(_Spotipy):
        def search(self, *, q: str, type: str, limit: int) -> Any:
            del type, limit
            self.queries.append(q)
            return {"tracks": {"items": []}}

    client = _Empty()

    assert (
        so.SpotifyTrackSearch(client).tracks("Rihanna & Calvin Harris", "We Found Love")
        == []
    )
    assert client.queries == [
        "artist:Rihanna & Calvin Harris track:We Found Love",
        "artist:Rihanna track:We Found Love",
    ]


def test_a_failed_spotify_search_raises_search_failed() -> None:
    try:
        so.SpotifyTrackSearch(_Spotipy(fails=True)).tracks("A", "B")
    except so.SearchFailed:
        return
    raise AssertionError("a failed search must not read as finding nothing")


def test_a_failed_search_is_no_second_opinion_not_neither() -> None:
    @dataclass
    class _Failing:
        def tracks(self, artist: str, title: str) -> list[Recording]:
            del artist, title
            raise so.SearchFailed("HTTPError")

    verdict = so.decide(*_BOOM_CLAP, *_SUNNY, Heard(169.0), _Failing())

    assert verdict.side is None
    assert verdict.why.startswith(so.NO_SPOTIFY)
    assert "neither" not in verdict.why
