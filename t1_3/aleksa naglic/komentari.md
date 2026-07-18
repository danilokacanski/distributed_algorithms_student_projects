# Aleksa Naglić — Tendermint konsenzus simulator (Rust)

---

## +:

- pravi distribuirani sistem, gde je svaki cvor docker container koji komunicira preko tcp-a
- 'Tokio async runtime' je petlja sa pinned timerom i ima ne-blokirajući TCP I/O
- odradjen UDP i ima websocket dashobard
- SHA-256 deduplication gossip sprecava beskonacnu petlju u gossip protkolu

---

## -:

- u 'handle_proposal' nedostaje 'locked_round <= valid_round' uslov , laka ispravka znao je na odbrani da to treba tako

- 'seen_messages: HashSet<String>' raste neograniceno, mada kaze da ne nastaje problem u analitci ni kada ukloni delay i odradei se visina 1000 kroz par sekundi

PREDLOG OCENE: 10