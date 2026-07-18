# Marija Ninkovic — ROS2 PBFT Emergency Stop (Python)

## +:
- ROS2 sistem, gde je svaka replika ROS2 cvor koji kroz DDS komunicira sa PubSub modelom
- svaki callback proverava digest pre prihvatanja poruke
- bufferuju se ranije commit i prepare poruke
- kompletno odradjen new-view i view-change
- 12 scenarija vizantijskog ponasanja
- dodatan cvor za nadgledanje safety svojstva
- odlicna vizuelizacija preko UML dijagrama koja je sama crtala
---

## -:

- mrtav kod u '_validate_view_change_certificate' koji biva prepisan sledecim blokom tako da funkcionalno ne pravli problem

- prepared sertifikat sadrzi samo id bez prepared poruke, tako da se ne moze verifikovati da je vrednost stvano bila prepared

PREDLOG OCENE: 10