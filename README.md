```markdown
<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,14,15,16,18&height=250&section=header&text=Soul%20Collector%20Bot&fontSize=70&fontAlignY=35&desc=Anime%20Character%20Collection%20%26%20Trading%20System&descAlignY=55&descAlign=62&animation=fadeIn" width="100%" />
</p>

<p align="center">
  <a href="https://python.org"><img src="https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python Version"/></a>
  <a href="https://fastapi.tiangolo.com"><img src="https://img.shields.io/badge/FastAPI-0.100+-00d57b?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI"/></a>
  <a href="https://postgresql.org"><img src="https://img.shields.io/badge/PostgreSQL-14+-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/></a>
  <a href="https://github.com/python-telegram-bot/python-telegram-bot"><img src="https://img.shields.io/badge/python--telegram--bot-20.0+-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white" alt="PTB"/></a>
</p>

<p align="center">
  <a href="https://t.me/+EJLXtbm4Qa4wNzFl"><img src="https://img.shields.io/badge/👤%20Owner-Contact-blue?style=flat-square&logo=telegram" alt="Owner"/></a>
  <a href="https://t.me/kingchaos7"><img src="https://img.shields.io/badge/💬%20Support-Chat-brightgreen?style=flat-square&logo=telegram" alt="Support Chat"/></a>
  <a href="https://t.me/yourryori"><img src="https://img.shields.io/badge/📢%20Channel-Updates-red?style=flat-square&logo=telegram" alt="Channel"/></a>
</p>

---

## 🎴 Overview

**Soul Collector** is a sophisticated, production-ready Telegram bot that brings the thrill of anime character collection to group chats. Featuring a weighted rarity system, real-time economy, automated drop mechanics, and a responsive web store interface.

### ✨ Key Features

| Feature | Description |
|---------|-------------|
| 🎲 **Gacha System** | Weighted probability drops with 10 rarity tiers |
| 💰 **Economy** | Coins system with daily claims, streaks & bonuses |
| 🏆 **Leaderboards** | Weekly competitions with automated prize distribution |
| 🎭 **Drop System** | Hourly character drops in groups with hint mechanics |
| 🛒 **Web Store** | Progressive Web App for browsing characters |
| 📋 **Tasks** | Achievement system with referrals & rewards |
| 🌍 **i18n** | Multi-language support (English & Russian) |
| ⚡ **Performance** | Async PostgreSQL with connection pooling |
| 🔒 **Admin Tools** | Comprehensive owner controls & broadcast system |

---

## 🏗️ Architecture

```mermaid
graph TB
    subgraph "Telegram Layer"
        TG[Telegram API]
        PTB[Python-Telegram-Bot]
    end
    
    subgraph "Application Layer"
        API[FastAPI]
        BOT[Bot Handlers]
        SCHED[APScheduler]
    end
    
    subgraph "Data Layer"
        DB[(PostgreSQL)]
        REDIS[Redis Optional]
    end
    
    subgraph "Client Layer"
        WEB[Web Store<br/>HTML/JS]
        MOBILE[Mobile Clients]
    end
    
    TG -->|Webhook/Polling| PTB
    PTB --> BOT
    BOT --> DB
    API --> DB
    SCHED -->|Hourly Drops| BOT
    WEB -->|REST API| API
    MOBILE -->|Bot| PTB
```

---

🚀 Quick Start

Prerequisites

- Python 3.10+
- PostgreSQL 14+
- Telegram Bot Token ([@BotFather](https://t.me/botfather))

Installation

```bash
# Clone repository
git clone https://github.com/yourusername/soul-collector.git
cd soul-collector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

Environment Variables

Create `.env` file:

```env
# Required
BOT_TOKEN=your_bot_token_here
DATABASE_URL=postgresql://user:pass@localhost:5432/soulcollector
OWNER_ID=7728424218
OWNER_USERNAME=YourUsername

# Optional
WEBHOOK_URL=https://your-domain.com
WEB_STORE_URL=https://your-vercel-app.vercel.app
PORT=8000
```

Database Setup

```sql
-- Tables are auto-created on first run via init_tables()
-- Manual migration example:
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```

---

📚 Command Reference

👤 User Commands

Command	Usage	Description	
`/start`	-	Initialize bot & register user	
`/help`	-	Show command menu	
`/daily`	-	Claim coins (2h cooldown)	
`/claim`	-	Claim random character (11h cooldown)	
`/wallet`	-	View balance & stats	
`/vault [page]`	-	Browse collection	
`/market`	-	Open web store	
`/buy <id>`	`/buy #0001`	Purchase character	
`/sell <id>`	`/sell #0001`	Sell for 70% value	
`/search <name>`	`/search Naruto`	Find characters	
`/guess <name>`	`/guess Goku`	Answer drop question	
`/tasks`	DM only	View achievements	
`/refer`	DM only	Get referral link	
`/leaderboard`	Groups only	View top collectors	
`/lang`	-	Change language	

👑 Owner Commands

Command	Privilege	Description	
`/addcharacter`	Owner	Interactive character creation	
`/remove <id>`	Owner	Delist character	
`/broadcast`	Owner	Mass message with media support	
`/addcoins`	Owner	Fund user (reply)	
`/stats`	Owner	Global analytics	
`/resetgrpdata`	Owner	Wipe group data	
`/setstartmsg`	Owner	Customize welcome text	

👥 Group Admin

Command	Requirement	Description	
`/enabledrops`	Admin	Start hourly drops	
`/disabledrops`	Admin	Stop drops	
`/listadmins`	Member	Show human admins	
`/calladmins`	Member	Ping admins (10m CD)	

---

🎮 Rarity System

Tier	Name	Weight	Bonus	Emoji	
1	Common	40	50⚪	⚪	
2	Uncommon	25	100🟢	🟢	
3	Elite	15	200🔵	🔵	
4	Epic	8	400🟣	🟣	
5	Mythic	0.5	2500🔴	🔴	
6	Waifu	4	500💖	💖	
7	Special Edition	3	700✨	✨	
8	Limited	1.5	1200⏳	⏳	
9	Event	1	1500🎉	🎉	
10	Legendary	2	1000🌟	🌟	

---

🌐 Web Store

The built-in web interface provides a mobile-first browsing experience:

```bash
# Access locally
http://localhost:8000/

# API Endpoint
GET /api/characters  # Returns JSON array
```

Features:
- 🔍 Real-time search (name, anime, ID)
- 🎌 Anime filter strip with counts
- 📱 Responsive card design
- ⚡ Vanilla JS (no frameworks needed)

---

⚙️ Deployment

Railway/Render (Recommended)

```yaml
# railway.yaml
build:
  builder: NIXPACKS
deploy:
  startCommand: python main.py
  healthcheckPath: /health
  healthcheckTimeout: 300
```

Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

Webhook Setup

```bash
# Set environment variable
WEBHOOK_URL=https://your-app.railway.app

# Bot automatically configures webhook at /webhook endpoint
```

---

📊 Database Schema

```mermaid
erDiagram
    GLOBAL_USERS ||--o{ INVENTORY : owns
    CHARACTERS ||--o{ INVENTORY : contained_in
    GROUPS ||--o{ GROUP_USER_DATA : tracks
    USERS ||--o{ GROUP_USER_DATA : participates
    
    GLOBAL_USERS {
        bigint user_id PK
        int coins
    }
    
    CHARACTERS {
        string char_id PK
        string name
        string anime
        string img_url
        string rarity
        int price
        boolean is_available
    }
    
    INVENTORY {
        bigint user_id FK
        bigint group_id FK
        string char_id FK
    }
    
    GROUP_USER_DATA {
        bigint user_id FK
        bigint group_id FK
        timestamp last_daily
        timestamp last_claim
        int daily_streak
    }
    
    CURRENT_DROPS {
        bigint group_id PK
        string char_id FK
        timestamp start_time
        bigint message_id
    }
```

---

🎯 Task System

Referral Chain:

```
Referrer → 1000 coins
Referred → 500 coins
```

Achievement Bonuses:
- 🎁 7-Day Streak: +3000 coins
- 🛒 First Purchase: +1500 coins  
- 🎴 Collector (10 chars): +2500 coins
- 🎯 First Guess: +1000 coins
- ➕ Add to Group: +5000 coins

---

🖼️ Screenshots

---

🔧 Configuration

Custom Welcome Message

```python
# Via /setstartmsg command (Owner only)
🎐 <b>ꜱᴏᴜʟ ᴄᴏʟʟᴇᴄᴛᴏʀ</b>

✦ Welcome, <b>{name}</b>

<i>Anime souls drift through the void…
Only the worthy may claim them.</i>
```

Drop Scheduling

```python
# Configurable in perform_drop()
scheduler.add_job(
    hourly_drops, 
    IntervalTrigger(hours=1)
)

# Weekly leaderboard: Sundays 12:00 UTC
```

---

🛡️ Error Handling

- Forbidden Errors: Auto-disable drops when bot removed
- Rate Limiting: 20msg/sec sleep intervals in broadcasts
- Connection Pools: Asyncpg with automatic reconnection
- Graceful Degradation: Fallback text when images fail

---

📈 Monitoring

Health check endpoint for Uptime Robot:

```
GET /health
→ {"status": "ok", "timestamp": "2026-04-08T21:10:00"}
```

---

🤝 Support

📢 Updates: [@yourryori](https://t.me/yourryori)

💬 Support Chat: [@kingchaos7](https://t.me/kingchaos7)

👤 Contact: [Owner](https://t.me/+EJLXtbm4Qa4wNzFl)

</div>
<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,14,15,16,18&height=150&section=footer&text=Collect%20%C2%B7%20Trade%20%C2%B7%20Dominate&fontSize=30&fontAlignY=70&animation=fadeIn" width="100%" />
</p>
<p align="center">
  <sub>Built with ❤️ and ☕ using Python-Telegram-Bot & FastAPI</sub>
</p>
```
This README features:
Dynamic headers/footers using Capsule Render
Mermaid diagrams for architecture & database visualization
Comprehensive tables for commands and rarities
Environment setup with copy-paste ready code blocks
Deployment guides for modern platforms (Railway/Render)
Your specific Telegram links integrated throughout
Professional badges for tech stack visualization
Mobile-responsive formatting considerations
Bruh I want all in code formet copy paste bruh
Edit
Copy
Share
Avatar
Kimi
Return README as raw markdown code block directly
Markdown
  
Code
Preview
Feature	Description
🎲 Gacha System	Weighted probability drops with 10 rarity tiers
💰 Economy	Coins system with daily claims, streaks & bonuses
🏆 Leaderboards	Weekly competitions with automated prize distribution
🎭 Drop System	Hourly character drops in groups with hint mechanics
🛒 Web Store	Progressive Web App for browsing characters
📋 Tasks	Achievement system with referrals & rewards
🌍 i18n	Multi-language support (English & Russian)
⚡ Performance	Async PostgreSQL with connection pooling
🔒 Admin Tools	Comprehensive owner controls & broadcast system
✨ Key Features
🎴 Overview
graph TB
    subgraph "Telegram Layer"
        TG[Telegram API]
        PTB[Python-Telegram-Bot]
    end
    
    subgraph "Application Layer"
        API[FastAPI]
        BOT[Bot Handlers]
        SCHED[APScheduler]
    end
    
    subgraph "Data Layer"
        DB[(PostgreSQL)]
        REDIS[Redis Optional]
    end
    
    subgraph "Client Layer"
        WEB[Web Store
HTML/JS]
        MOBILE[Mobile Clients]
    end
    
    TG -->|Webhook/Polling| PTB
    PTB --> BOT
    BOT --> DB
    API --> DB
    SCHED -->|Hourly Drops| BOT
    WEB -->|REST API| API
    MOBILE -->|Bot| PTB
🏗️ Architecture
🚀 Quick Start
Prerequisites
Python 3.10+
PostgreSQL 14+
Telegram Bot Token (@BotFather)
Installation
bash
Copy
# Clone repository
git clone https://github.com/yourusername/soul-collector.git
cd soul-collector

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
Environment Variables
Create .env file:
env
Copy
# Required
BOT_TOKEN=your_bot_token_here
DATABASE_URL=postgresql://user:pass@localhost:5432/soulcollector
OWNER_ID=7728424218
OWNER_USERNAME=YourUsername

# Optional
WEBHOOK_URL=https://your-domain.com
WEB_STORE_URL=https://your-vercel-app.vercel.app
PORT=8000
Database Setup
sql
Copy
-- Tables are auto-created on first run via init_tables()
-- Manual migration example:
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
📚 Command Reference
👤 User Commands
Table
Command	Usage	Description
/start	-	Initialize bot & register user
/help	-	Show command menu
/daily	-	Claim coins (2h cooldown)
/claim	-	Claim random character (11h cooldown)
/wallet	-	View balance & stats
/vault [page]	-	Browse collection
/market	-	Open web store
/buy <id>	/buy #0001	Purchase character
/sell <id>	/sell #0001	Sell for 70% value
/search <name>	/search Naruto	Find characters
/guess <name>	/guess Goku	Answer drop question
/tasks	DM only	View achievements
/refer	DM only	Get referral link
/leaderboard	Groups only	View top collectors
/lang	-	Change language
👑 Owner Commands
Table
Command	Privilege	Description
/addcharacter	Owner	Interactive character creation
/remove <id>	Owner	Delist character
/broadcast	Owner	Mass message with media support
/addcoins	Owner	Fund user (reply)
/stats	Owner	Global analytics
/resetgrpdata	Owner	Wipe group data
/setstartmsg	Owner	Customize welcome text
👥 Group Admin
Table
Command	Requirement	Description
/enabledrops	Admin	Start hourly drops
/disabledrops	Admin	Stop drops
/listadmins	Member	Show human admins
/calladmins	Member	Ping admins (10m CD)
🎮 Rarity System
Table
Tier	Name	Weight	Bonus	Emoji
1	Common	40	50⚪	⚪
2	Uncommon	25	100🟢	🟢
3	Elite	15	200🔵	🔵
4	Epic	8	400🟣	🟣
5	Mythic	0.5	2500🔴	🔴
6	Waifu	4	500💖	💖
7	Special Edition	3	700✨	✨
8	Limited	1.5	1200⏳	⏳
9	Event	1	1500🎉	🎉
10	Legendary	2	1000🌟	🌟
🌐 Web Store
The built-in web interface provides a mobile-first browsing experience:
bash
Copy
# Access locally
http://localhost:8000/

# API Endpoint
GET /api/characters  # Returns JSON array
Features:
🔍 Real-time search (name, anime, ID)
🎌 Anime filter strip with counts
📱 Responsive card design
⚡ Vanilla JS (no frameworks needed)
⚙️ Deployment
Railway/Render (Recommended)
yaml
Copy
# railway.yaml
build:
  builder: NIXPACKS
deploy:
  startCommand: python main.py
  healthcheckPath: /health
  healthcheckTimeout: 300
Docker
dockerfile
Copy
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
Webhook Setup
bash
Copy
# Set environment variable
WEBHOOK_URL=https://your-app.railway.app

# Bot automatically configures webhook at /webhook endpoint
📊 Database Schema
Mermaid
  
Code
Preview
owns

contained_in

tracks

participates

GLOBAL_USERS
bigint
user_id
PK
int
coins
INVENTORY
bigint
user_id
FK
bigint
group_id
FK
string
char_id
FK
CHARACTERS
string
char_id
PK
string
name
string
anime
string
img_url
string
rarity
int
price
boolean
is_available
GROUPS
GROUP_USER_DATA
bigint
user_id
FK
bigint
group_id
FK
timestamp
last_daily
timestamp
last_claim
int
daily_streak
USERS
CURRENT_DROPS
bigint
group_id
PK
string
char_id
FK
timestamp
start_time
bigint
message_id
🎯 Task System
Referral Chain:
plain
Copy
Referrer → 1000 coins
Referred → 500 coins
Achievement Bonuses:
🎁 7-Day Streak: +3000 coins
🛒 First Purchase: +1500 coins
🎴 Collector (10 chars): +2500 coins
🎯 First Guess: +1000 coins
➕ Add to Group: +5000 coins
🖼️ Screenshots
<p align="center">
  <img src="https://via.placeholder.com/300x600/1a1a2e/ffffff?text=Mobile+Interface" width="30%" />
  <img src="https://via.placeholder.com/300x600/1a1a2e/ffffff?text=Character+Card" width="30%" />
  <img src="https://via.placeholder.com/300x600/1a1a2e/ffffff?text=Leaderboard" width="30%" />
</p>
🔧 Configuration
Custom Welcome Message
Python
Copy
# Via /setstartmsg command (Owner only)
🎐 <b>ꜱᴏᴜʟ ᴄᴏʟʟᴇᴄᴛᴏʀ</b>

✦ Welcome, <b>{name}</b>

<i>Anime souls drift through the void…
Only the worthy may claim them.</i>
Drop Scheduling
Python
Copy
# Configurable in perform_drop()
scheduler.add_job(
    hourly_drops, 
    IntervalTrigger(hours=1)
)

# Weekly leaderboard: Sundays 12:00 UTC
🛡️ Error Handling
Forbidden Errors: Auto-disable drops when bot removed
Rate Limiting: 20msg/sec sleep intervals in broadcasts
Connection Pools: Asyncpg with automatic reconnection
Graceful Degradation: Fallback text when images fail
📈 Monitoring
Health check endpoint for Uptime Robot:
plain
Copy
GET /health
→ {"status": "ok", "timestamp": "2026-04-08T21:10:00"}
🤝 Support
<div align="center">
📢 Updates: @yourryori
💬 Support Chat: @kingchaos7
👤 Contact: Owner
</div>
<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=12,14,15,16,18&height=150&section=footer&text=Collect%20%C2%B7%20Trade%20%C2%B7%20Dominate&fontSize=30&fontAlignY=70&animation=fadeIn" width="100%" />
</p>
<p align="center">
  <sub>Built with ❤️ and ☕ using Python-Telegram-Bot & FastAPI</sub>
</p>
```