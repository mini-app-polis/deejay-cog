"""A second opinion when a library file's fingerprint and its own tags
disagree.

AcoustID's answer for a file is the best-scoring recording linked to its
audio, and those links are crowd-sourced: the best one can be another song,
another version of the song, or the right song spelled or credited
differently. A file's own tags can be wrong too (a remix tagged as the
original, a credit left as "Various Artist"). Neither side wins by itself;
the file goes one way only when a second source agrees with it:

1. **The other AcoustID recordings.** One fingerprint usually matches
   several recordings of the same audio (the original, a radio edit, a
   compilation copy). When any of them is the song and version the file's
   tags name, the fingerprint confirms the tags.
2. **Spotify's length.** Each side is searched on Spotify, and a track that
   is that side's song and version, within :data:`LENGTH_TOLERANCE_SECS` of
   the file's own length, backs it. When exactly one side is backed, that
   side wins.

Anything else (both backed, neither, or no Spotify) stays for the owner,
with what each source said in its note. When Spotify could not be asked (no
client, or a search that failed), the note says so rather than "neither", so
a failed lookup is not mistaken for an answer, and the file is tried again.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from mini_app_polis import logger as logger_mod

from deejay_cog.music_tags import tags_agree

log = logger_mod.get_logger()

#: How far a Spotify track's length may be from the file's and still be the
#: same recording: encoders and trimmed silence move it by a second or two;
#: another edit or version is usually further apart.
LENGTH_TOLERANCE_SECS = 3.0

#: Spotify tracks looked at per search.
SPOTIFY_RESULTS = 5

Side = Literal["tags", "fingerprint"]

#: How a note begins when Spotify was not asked (no client, or a search
#: failed): the second opinion was not taken, so the file is tried again.
NO_SPOTIFY = "no second opinion: Spotify"


class SearchFailed(Exception):
    """A track search could not be made (network, token, rate limit): no
    answer, which is not the same as finding nothing."""


@dataclass(frozen=True)
class Recording:
    """One recording a source names: its title, artist and length."""

    title: str
    artist: str
    duration: float | None = None


@dataclass(frozen=True)
class Heard:
    """What the fingerprint lookup heard: the file's length and every
    recording AcoustID linked to its audio, best first."""

    duration: float | None
    recordings: tuple[Recording, ...] = ()


@dataclass(frozen=True)
class Verdict:
    """Which side a second source backs (``None``: neither, for the
    owner), and why, for the file's note."""

    side: Side | None
    why: str


class TrackSearch(Protocol):
    """A catalog of released tracks to search by artist and title."""

    def tracks(self, artist: str, title: str) -> list[Recording]:
        """Tracks matching ``artist`` and ``title``, best first; empty when
        there are none. Raises :class:`SearchFailed` when the search could
        not be made."""
        ...


def _minutes(secs: float) -> str:
    whole = int(round(secs))
    return f"{whole // 60}:{whole % 60:02d}"


def acoustid_recordings(answer: dict[str, Any]) -> tuple[Recording, ...]:
    """Every recording with a title in an AcoustID lookup answer
    (``meta=recordings``), best result first."""
    found: list[tuple[float, Recording]] = []
    for result in answer.get("results") or []:
        score = float(result.get("score") or 0.0)
        for recording in result.get("recordings") or []:
            title = str(recording.get("title") or "").strip()
            artist = "".join(
                f"{a.get('name') or ''}{a.get('joinphrase') or ''}"
                for a in recording.get("artists") or []
            ).strip()
            if not title or not artist:
                continue
            duration = recording.get("duration")
            found.append(
                (
                    score,
                    Recording(
                        title, artist, float(duration) if duration is not None else None
                    ),
                )
            )
    found.sort(key=lambda pair: pair[0], reverse=True)
    return tuple(recording for _, recording in found)


def _backed(
    search: TrackSearch, artist: str, title: str, duration: float
) -> Recording | None:
    """A Spotify track that is ``artist`` - ``title`` (song and version) at
    the file's length, or None."""
    for track in search.tracks(artist, title):
        if (
            track.duration is not None
            and abs(track.duration - duration) <= LENGTH_TOLERANCE_SECS
            and tags_agree(title, artist, track.title, track.artist)
        ):
            return track
    return None


def decide(
    own_title: str,
    own_artist: str,
    found_title: str,
    found_artist: str,
    heard: Heard | None,
    search: TrackSearch | None,
) -> Verdict:
    """The second opinion on a file whose tags (``own_*``) and fingerprint
    (``found_*``) disagree."""
    for recording in heard.recordings if heard else ():
        if tags_agree(own_title, own_artist, recording.title, recording.artist):
            return Verdict(
                "tags",
                f"AcoustID also links this audio to {recording.artist} - "
                f"{recording.title}, the file's own song",
            )
    if heard is None or heard.duration is None:
        return Verdict(None, "no second opinion: the file's length is unknown")
    if search is None:
        return Verdict(None, f"{NO_SPOTIFY} is not available")
    length = _minutes(heard.duration)
    try:
        own = _backed(search, own_artist, own_title, heard.duration)
        found = _backed(search, found_artist, found_title, heard.duration)
    except SearchFailed as exc:
        return Verdict(None, f"{NO_SPOTIFY} search failed ({exc})")
    if own is not None and found is None:
        return Verdict(
            "tags",
            f"Spotify has {own.artist} - {own.title} at the file's length ({length})",
        )
    if found is not None and own is None:
        return Verdict(
            "fingerprint",
            f"Spotify has {found.artist} - {found.title} at the file's length "
            f"({length}), and not the file's own song",
        )
    if own is not None and found is not None:
        return Verdict(None, f"Spotify has both songs at the file's length ({length})")
    return Verdict(None, f"Spotify has neither song at the file's length ({length})")


#: What follows the first credited artist: a search names only that one.
_MORE_ARTISTS = re.compile(
    r"\s*(?:,|&|\bx\b|\bfeat\.?|\bft\.?|\bfeaturing\b|\bwith\b).*$",
    re.IGNORECASE,
)

#: A title's bracketed or " - " parts: a search asks for the song, and the
#: version is checked on what comes back.
_TITLE_PARTS = re.compile(r"\s*(?:\([^)]*\)|\[[^\]]*\]|\s-\s.*$)")


def search_terms(artist: str, title: str) -> tuple[str, str]:
    """The first credited artist and the bare song title, as searched when
    the whole credit finds nothing."""
    main = _MORE_ARTISTS.sub("", artist).strip() or artist.strip()
    song = _TITLE_PARTS.sub("", title).strip() or title.strip()
    return main, song


class SpotifyTrackSearch:
    """:class:`TrackSearch` over Spotify's search, through a spotipy client."""

    def __init__(self, client: Any, *, limit: int = SPOTIFY_RESULTS) -> None:
        self._client = client
        self._limit = limit

    def tracks(self, artist: str, title: str) -> list[Recording]:
        """Spotify's tracks for ``artist`` and ``title``; empty when it has
        none. Raises :class:`SearchFailed` when a search cannot be made."""
        main, song = search_terms(artist, title)
        # The whole credit first ("Royal & the Serpent" is one name), then
        # the first artist alone ("Rihanna & Calvin Harris" is two).
        for name in dict.fromkeys((artist.strip(), main)):
            found = self._search(name, song)
            if found:
                return found
        return []

    def _search(self, artist: str, song: str) -> list[Recording]:
        try:
            answer = self._client.search(
                q=f"artist:{artist} track:{song}", type="track", limit=self._limit
            )
        except Exception as exc:  # noqa: BLE001 — no opinion, not a failed run
            log.warning("second opinion: Spotify search failed: %s", exc)
            raise SearchFailed(type(exc).__name__) from exc
        out = []
        for item in (answer or {}).get("tracks", {}).get("items") or []:
            name = str(item.get("name") or "").strip()
            artists = ", ".join(
                str(a.get("name") or "") for a in item.get("artists") or []
            ).strip(", ")
            ms = item.get("duration_ms")
            if name and artists:
                out.append(
                    Recording(name, artists, ms / 1000 if ms is not None else None)
                )
        return out
