# 📚 Notes Search Bot (Telegram)

Ek channel me posted notes (text/photos/PDFs) ko index karta hai aur group me
**accurate search** karke sahi notes de deta hai. Search SQLite FTS5
(full-text) + fuzzy matching se hoti hai, isliye thode alag spelling ya extra
words ke saath bhi sahi result milta hai.

## Setup (5 minute)

1. **Python 3.9+** install karo, phir:
   ```
   pip install -r requirements.txt
   ```

2. **Ek hi command me pura setup:**
   ```
   python setup.py
   ```
   Ye script poochhegi:
   - `API_ID` / `API_HASH` — [my.telegram.org](https://my.telegram.org) → *API development tools* se banao
   - `BOT_TOKEN` — Telegram me [@BotFather](https://t.me/BotFather) kholo → `/newbot` → token do
   - `CHANNEL` — notes channel ka username (jaise `@my_notes_channel`) ya id

   Phir **ek baar Telegram login** karayegi (phone number + code) aur
   `SESSION_STRING` khud `.env` me likh degi. Isi session se bot channel ke
   **purane notes automatic index** karta hai (bots ko Telegram history padhne
   ki permission nahi deta, isliye ye ek baar login zaroori hai).

3. **Bot ko channel me ADMIN banao** (admin rights kaafi hain — post karne ki
   zaroorat nahi). Isse channel ka har **naya post real-time index** hoga.

4. **Bot ko group me add karo.** Ab koi bhi likh sakta hai:
   ```
   /find physics ch 5 semiconductors
   ```
   Bot turant file nahi bhejta — pehle **results buttons me** dikhata hai
   (title ke saath, **page me 5 ke hisaab se**, aur "Agla ▶️" se next page),
   jo note sahi ho uspe **tap** karo, file/text mil jayega. Isse galat file
   ka chance nahi rehta.
   Kisi bhi chat me inline: `@yourbotname physics ch 5`
   (inline ke liye BotFather me `/setinline` on karna hoga.)

5. Bot chalu karo: `python bot.py` — startup pe bot khud missing (purane)
   notes fetch kar lega, har baar sirf naye/missing wale.

## Commands

| Command | Kaam |
|---|---|
| `/find <query>` | Group me notes search karo (top 5 results, files direct bhejta hai) |
| `@yourbotname <query>` | Inline search — kisi bhi chat se |
| `/stats` | Index me kitne notes hain |
| `/reindex` | Channel history ka missing hissa abhi index karo (SESSION_STRING chahiye; startup pe bhi automatic hota hai) |
| `/start`, `/help` | Help message |

## Search kaisi kaam karti hai (accuracy)

- Har note ka **caption/text + file name** index hota hai (SQLite FTS5).
- Query ke tokens ke **prefix matches** (`semicon` → `semiconductor`).
- Phir **fuzzy reranking**: token coverage, spelling similarity
  (typos handle karta hai), title match bonus, naye posts ka thoda bonus.
- Common filler words (`pdf`, `notes`, `chapter`, `hai`, `ke`...) ignore hote
  hain, isliye "class 12 physics ch 5 pdf" bhi "physics semiconductor" bhi
  kaam karta hai.
- Stopwords wali list aap `bot.py` ke `STOPWORDS` me apne hisaab se badal sakte ho.

## Chalana

```
python bot.py
```

24×7 chalane ke liye kisi VPS pe `systemd` ya `screen`/`tmux` me chalao.

## Notes

- Files wahi se bheji jaati hain jahan channel me hain (duplicate download
  nahi hota) — isliye bot ko channel me admin hona zaroori hai.
- `GROUP_ID` bhar do to bot sirf usi group me `/find` accept karega; khali
  chhodo to har group me kaam karega.
- Index `notes_index.db` me local save hota hai — restart pe safe.
