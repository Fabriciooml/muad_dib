# Cinema Session Tracker

Tracker watches provider listings for scheduled screenings and records first discovery of each session.

## Language

**Provider**:
A cinema company or listing source whose catalog and sessions tracker can inspect. Cineart is first provider.
_Avoid_: Cinema

**Cinema**:
A physical venue where a session takes place. One provider may list several cinemas.
_Avoid_: Provider

**Room**:
A named auditorium within a cinema, such as Sala 06 IMAX.
_Avoid_: Cinema

**Watch**:
A saved request to monitor one movie title across selected providers and optional location filters at a chosen interval.
_Avoid_: Job

**Provider target**:
One watch's movie resolution within one provider. It can remain pending until that provider lists the movie.
_Avoid_: Watch

**Session**:
One scheduled screening of a movie at a cinema, room, date, and time, according to a provider.
_Avoid_: Ticket, showing availability

**Discovery**:
First time tracker stores a session identity. Later changes to its purchase URL remain updates to same session.
_Avoid_: Purchase, notification
