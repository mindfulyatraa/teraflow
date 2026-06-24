# ============================================================
# TERABOX TURBO PRO - SERVERLESS & CLOUDFLARE OPTIMIZED (V5.0.0)
# Designed for Auto-scaling (Vercel) & Edge Caching (Cloudflare)
# ============================================================

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
import asyncio
import aiohttp
import json
import time
import hashlib
import uuid
import logging
import os
import random
import re
import requests
from contextlib import asynccontextmanager
import uvicorn

# Try imports for optional database components
try:
    import asyncpg
except ImportError:
    asyncpg = None

try:
    import redis.asyncio as aioredis
except ImportError:
    try:
        import aioredis
    except ImportError:
        aioredis = None

# ============================================================
# CONFIGURATION
# ============================================================

class Config:
    # Redis (For high-speed shared cache)
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', '')
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    
    # PostgreSQL (For endpoint and config management)
    PG_HOST = os.getenv('PG_HOST', 'localhost')
    PG_PORT = int(os.getenv('PG_PORT', 5432))
    PG_DB = os.getenv('PG_DB', 'terabox')
    PG_USER = os.getenv('PG_USER', 'postgres')
    PG_PASSWORD = os.getenv('PG_PASSWORD', '')
    PG_POOL_MIN = int(os.getenv('PG_POOL_MIN', 2))
    PG_POOL_MAX = int(os.getenv('PG_POOL_MAX', 20))
    
    # Cache Configuration
    CACHE_TTL = int(os.getenv('CACHE_TTL', 1800))  # 30 Minutes
    
    # Timeout Settings
    TIMEOUT = int(os.getenv('TIMEOUT', 3))
    
    # Terabox configurations
    TERABOX_APP_ID = os.getenv('TERABOX_APP_ID', '250528')

config = Config()

# ============================================================
# LOGGING (Console-only for Serverless logs collector compatibility)
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ============================================================
# ASYNC DATABASE (asyncpg with mock fallback)
# ============================================================

class AsyncDatabase:
    def __init__(self):
        self.pool = None
        self.use_mock = False
    
    async def connect(self):
        if self.use_mock:
            return None
        if self.pool is None:
            if asyncpg is None:
                logger.warning("asyncpg not installed. Using mock database fallback.")
                self.use_mock = True
                return None
            try:
                self.pool = await asyncpg.create_pool(
                    host=config.PG_HOST,
                    port=config.PG_PORT,
                    database=config.PG_DB,
                    user=config.PG_USER,
                    password=config.PG_PASSWORD,
                    min_size=config.PG_POOL_MIN,
                    max_size=config.PG_POOL_MAX,
                    max_inactive_connection_lifetime=180,
                    command_timeout=30
                )
            except Exception as e:
                logger.error(f"PostgreSQL connection failed: {e}. Using mock database fallback.")
                self.use_mock = True
        return self.pool
    
    async def execute(self, query: str, *args):
        pool = await self.connect()
        if self.use_mock:
            return None
        async with pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        pool = await self.connect()
        if self.use_mock:
            return []
        async with pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def close(self):
        if self.pool:
            await self.pool.close()

db = AsyncDatabase()

# ============================================================
# ASYNC REDIS (aioredis with mock fallback)
# ============================================================

class AsyncRedis:
    def __init__(self):
        self.client = None
        self.use_mock = False
        self._fallback_cache = {}
        self._fallback_cache_ttl = {}
    
    async def connect(self):
        if self.use_mock:
            return None
        if self.client is None:
            if aioredis is None:
                logger.warning("aioredis/redis not installed. Using mock in-memory cache.")
                self.use_mock = True
                return None
            try:
                self.client = await aioredis.from_url(
                    f"redis://{config.REDIS_HOST}:{config.REDIS_PORT}/{config.REDIS_DB}",
                    password=config.REDIS_PASSWORD,
                    decode_responses=True,
                    max_connections=20,
                    socket_timeout=3,
                    retry_on_timeout=True
                )
            except Exception as e:
                logger.error(f"Redis connection failed: {e}. Using mock in-memory cache.")
                self.use_mock = True
        return self.client
    
    async def get(self, key: str) -> Optional[str]:
        redis = await self.connect()
        if self.use_mock:
            if key in self._fallback_cache:
                if self._fallback_cache_ttl.get(key, 0) > time.time():
                    return self._fallback_cache[key]
                else:
                    del self._fallback_cache[key]
                    del self._fallback_cache_ttl[key]
            return None
        try:
            return await redis.get(key)
        except Exception:
            return None
    
    async def set(self, key: str, value: str, ex: int = None):
        redis = await self.connect()
        if self.use_mock:
            self._fallback_cache[key] = value
            if ex:
                self._fallback_cache_ttl[key] = time.time() + ex
            return True
        try:
            if ex:
                return await redis.setex(key, ex, value)
            return await redis.set(key, value)
        except Exception:
            return False

redis_client = AsyncRedis()

# ============================================================
# PROXY MANAGER (Anti-IP-Ban with Non-blocking Startup)
# ============================================================

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.failed_proxies = set()
        self.current_index = 0
    
    async def initialize(self):
        """Asynchronously load proxies during startup"""
        env_proxies = os.getenv('PROXY_LIST')
        if env_proxies:
            self.proxies = [p.strip() for p in env_proxies.split(',') if p.strip()]
            logger.info(f"Loaded {len(self.proxies)} proxies from PROXY_LIST")
            return

        try:
            if os.path.exists('proxies.txt'):
                with open('proxies.txt', 'r') as f:
                    self.proxies = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                logger.info(f"Loaded {len(self.proxies)} proxies from proxies.txt")
                return
        except Exception as e:
            logger.error(f"Error reading proxies.txt: {e}")

        # Fetch public fallback proxies asynchronously
        sources = [
            "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
            "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        ]
        
        async with aiohttp.ClientSession() as session:
            for source in sources:
                try:
                    async with session.get(source, timeout=5) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            for line in text.split('\n'):
                                line = line.strip()
                                if line and ':' in line:
                                    self.proxies.append(f"http://{line}")
                except Exception:
                    pass
        
        if not self.proxies:
            self.proxies.extend([
                "http://45.137.22.43:8080",
                "http://103.152.112.120:80",
                "http://43.134.68.46:8080",
                "http://194.36.193.65:3128",
                "http://103.118.47.110:8080",
            ])
        
        random.shuffle(self.proxies)
        logger.info(f"Loaded {len(self.proxies)} proxies")
    
    async def get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        for _ in range(len(self.proxies)):
            proxy = self.proxies[self.current_index % len(self.proxies)]
            self.current_index += 1
            if proxy not in self.failed_proxies:
                return proxy
        
        self.failed_proxies.clear()
        return self.proxies[0] if self.proxies else None
    
    async def mark_failed(self, proxy: str):
        self.failed_proxies.add(proxy)

proxy_manager = ProxyManager()

# ============================================================
# FASTAPI APP LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Terabox Turbo Pro starting...")
    await db.connect()
    await redis_client.connect()
    await proxy_manager.initialize()
    await initialize_database()
    logger.info("All services connected")
    yield
    # Shutdown
    logger.info("Shutting down...")
    await db.close()
    await redis_client.close()

app = FastAPI(
    title="Terabox Turbo Pro",
    description="Production-grade Terabox streaming engine",
    version="5.0.0",
    lifespan=lifespan
)

# ============================================================
# MIDDLEWARE
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=86400,
)

# ============================================================
# MODELS
# ============================================================

class TeraboxRequest(BaseModel):
    url: str
    quality: Optional[str] = "hd"

class TeraboxBulkRequest(BaseModel):
    urls: list[str]
    quality: Optional[str] = "hd"

# ============================================================
# DATABASE INIT
# ============================================================

async def initialize_database():
    try:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS terabox_endpoints (
                id SERIAL PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                priority INT DEFAULT 1
            )
        """)
        
        # Populate default endpoints if database is empty
        endpoints_count = await db.fetch("SELECT COUNT(*) FROM terabox_endpoints")
        if endpoints_count and endpoints_count[0][0] == 0:
            default_endpoints = [
                'https://www.terabox.com/api/v1/file/download',
                'https://www.terabox.com/api/v2/file/download',
                'https://d.dubox.com/api/v1/file/download',
                'https://terabox.com/api/download'
            ]
            for endpoint in default_endpoints:
                await db.execute("INSERT INTO terabox_endpoints (url) VALUES ($1) ON CONFLICT DO NOTHING", endpoint)
        
        logger.info("Database tables ready")
    except Exception as e:
        logger.error(f"Database init error: {e}")

# ============================================================
# TERABOX ENGINE (With Proxy)
# ============================================================

class TeraboxEngine:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
        })
        self._cache_hits = 0
        self._cache_misses = 0
    
    def _get_cache_key(self, url: str, quality: str = 'hd') -> str:
        return f"terabox:{hashlib.md5(f'{url}:{quality}'.encode()).hexdigest()}"
    
    async def extract_info(self, share_url: str, quality: str = 'hd') -> dict:
        """Extract file info with proxy rotation and caching"""
        cache_key = self._get_cache_key(share_url, quality)
        
        # Check Redis cache
        cached = await redis_client.get(cache_key)
        if cached:
            self._cache_hits += 1
            data = json.loads(cached)
            data['cached'] = True
            return data
        
        self._cache_misses += 1
        
        # Fetch with proxy rotation
        html = None
        for attempt in range(2):
            proxy = await proxy_manager.get_proxy()
            proxies = {'http': proxy, 'https': proxy} if proxy else None
            
            try:
                resp = self.session.get(share_url, proxies=proxies, timeout=config.TIMEOUT)
                if resp.status_code == 200:
                    html = resp.text
                    break
                elif resp.status_code in [403, 429, 503]:
                    if proxy:
                        await proxy_manager.mark_failed(proxy)
                    continue
            except Exception as e:
                if proxy:
                    await proxy_manager.mark_failed(proxy)
                if attempt == 1:
                    raise
                await asyncio.sleep(0.5 * (attempt + 1))
        
        if not html:
            raise Exception("Failed to fetch Terabox page after retries")
        
        # Parse HTML
        data = await self._parse_html(html, quality)
        
        # Cache in Redis
        await redis_client.set(cache_key, json.dumps(data), ex=config.CACHE_TTL)
        
        return data
    
    async def _parse_html(self, html: str, quality: str = 'hd') -> dict:
        """Parse Terabox HTML and extract data"""
        data = {}
        
        # Extract fs_id
        fs_id = None
        patterns = [
            r'"fs_id":"(\d+)"',
            r'fs_id=(\d+)',
            r'"file_id":"(\d+)"',
            r'data-fs-id="(\d+)"'
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                fs_id = match.group(1)
                break
        if not fs_id:
            raise Exception("File ID not found")
        data['fs_id'] = fs_id
        
        # Extract sign
        sign = None
        patterns = [
            r'"sign":"([a-f0-9]+)"',
            r'sign=([a-f0-9]+)',
            r'"token":"([^"]+)"'
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                sign = match.group(1)
                break
        if not sign:
            raise Exception("Sign token not found")
        data['sign'] = sign
        
        # Extract filename
        name = None
        patterns = [
            r'"server_filename":"([^"]+)"',
            r'<title>([^<]+)',
            r'"filename":"([^"]+)"'
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                name = match.group(1).strip()
                break
        if not name:
            name = 'terabox_file.mp4'
        data['filename'] = name
        
        # Extract filesize
        size = 0
        patterns = [
            r'"size":(\d+)',
            r'size=(\d+)',
            r'"file_size":(\d+)'
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                size = int(match.group(1))
                break
        data['filesize'] = size
        
        # Extract thumbnail
        thumb = ''
        patterns = [
            r'"thumb":"([^"]+)"',
            r'"thumbnail":"([^"]+)"'
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                thumb = match.group(1)
                break
        data['thumbnail'] = thumb
        
        # Get direct download URL
        direct_url = await self._get_direct_url(fs_id, sign, quality)
        data['direct_url'] = direct_url
        
        return data
    
    async def _get_direct_url(self, fs_id: str, sign: str, quality: str = 'hd') -> str:
        """Get direct CDN URL with dynamic endpoints"""
        timestamp = int(time.time() * 1000)
        params = {
            'fs_id': fs_id,
            'sign': sign,
            'timestamp': timestamp,
            'logid': f'web_{timestamp}_{uuid.uuid4().hex[:8]}',
            'app_id': config.TERABOX_APP_ID,
            'channel': 'dubox',
            'clienttype': '0',
        }
        
        endpoints = []
        try:
            rows = await db.fetch("SELECT url FROM terabox_endpoints WHERE is_active = TRUE ORDER BY priority DESC")
            endpoints = [row['url'] for row in rows]
        except Exception:
            pass

        if not endpoints:
            endpoints = [
                'https://www.terabox.com/api/v1/file/download',
                'https://www.terabox.com/api/v2/file/download',
                'https://d.dubox.com/api/v1/file/download',
                'https://terabox.com/api/download',
            ]
        
        for endpoint in endpoints:
            try:
                proxy = await proxy_manager.get_proxy()
                proxies = {'http': proxy, 'https': proxy} if proxy else None
                resp = self.session.get(endpoint, params=params, proxies=proxies, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    url = data.get('dlink') or data.get('url') or data.get('download_url')
                    if url:
                        # Speed optimizations
                        url = url.replace('&dl=0', '&dl=1')
                        url = url.replace('&accelerate=0', '&accelerate=1')
                        return url
            except Exception:
                continue
        
        raise Exception("All API endpoints failed")
    
    async def get_stream_url(self, share_url: str, quality: str = 'hd') -> str:
        info = await self.extract_info(share_url, quality)
        direct_url = info.get('direct_url')
        if not direct_url:
            raise Exception("No direct URL available")
        
        if '?' in direct_url:
            direct_url += '&range=0-'
        else:
            direct_url += '?range=0-'
        
        return direct_url

engine = TeraboxEngine()

# ============================================================
# API ENDPOINTS (With Cloudflare/Vercel Cache Headers)
# ============================================================

@app.get("/")
async def root():
    return {
        "status": "Terabox Turbo Pro is running",
        "version": "5.0.0",
        "features": [
            "Serverless ready (No local temp files)",
            "Cloudflare Edge Caching compatible",
            "Proxy auto-rotation",
            "Async database & Redis cache fallbacks",
            "300k+ daily users scale capacity"
        ]
    }

@app.get("/health")
async def health(response: Response):
    # Cloudflare should not cache health check
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    return {
        "status": "healthy",
        "version": "5.0.0"
    }

@app.post("/api/terabox/info")
async def get_terabox_info(request: TeraboxRequest, response: Response):
    """Get file info + direct download URL"""
    try:
        allowed_domains = ['terabox', '1024tera', 'terafree', 'nephobox', 'dubox']
        if not any(domain in request.url for domain in allowed_domains):
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "Invalid Terabox URL"}
            )
        
        info = await engine.extract_info(request.url, request.quality)
        
        # Cache-Control headers for Cloudflare / Vercel Edge caching (30 minutes cache)
        response.headers["Cache-Control"] = f"public, max-age={config.CACHE_TTL}, s-maxage={config.CACHE_TTL}"
        
        return {
            "success": True,
            "data": {
                "filename": info.get('filename'),
                "filesize": info.get('filesize'),
                "thumbnail": info.get('thumbnail'),
                "direct_url": info.get('direct_url'),
                "stream_url": f"/api/terabox/stream?url={request.url}&quality={request.quality}",
                "cached": info.get('cached', False)
            }
        }
    except Exception as e:
        logger.error(f"API error: {e}")
        return {"success": False, "error": str(e)}

@app.post("/api/terabox/bulk")
async def get_bulk_info(request: TeraboxBulkRequest, response: Response):
    """Get info for multiple URLs"""
    results = []
    for url in request.urls[:30]:  # Cap at 30 for serverless execution duration limits
        try:
            info = await engine.extract_info(url, request.quality)
            results.append({
                "url": url,
                "success": True,
                "data": {
                    "filename": info.get('filename'),
                    "filesize": info.get('filesize'),
                    "thumbnail": info.get('thumbnail'),
                    "direct_url": info.get('direct_url')
                }
            })
        except Exception as e:
            results.append({"url": url, "success": False, "error": str(e)})
    
    # Cache bulk responses for 10 minutes
    response.headers["Cache-Control"] = "public, max-age=600, s-maxage=600"
    return {"success": True, "total": len(results), "results": results}

@app.get("/api/terabox/stream")
async def stream_terabox(url: str, quality: str = "hd"):
    """Stream video directly from CDN through serverless streaming pipes"""
    try:
        info = await engine.extract_info(url, quality)
        direct_url = info.get('direct_url')
        filename = info.get('filename', 'video.mp4')
        
        # Streaming generator to pipe Terabox video directly to client without downloading on server
        async def stream_generator():
            proxy = await proxy_manager.get_proxy()
            async with aiohttp.ClientSession() as session:
                async with session.get(direct_url, proxy=proxy) as resp:
                    async for chunk in resp.content.iter_chunked(256 * 1024):  # 256KB chunks for smooth buffering
                        yield chunk
        
        return StreamingResponse(
            stream_generator(),
            media_type='video/mp4',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Cache-Control': 'public, max-age=3600',
                'Accept-Ranges': 'bytes'
            }
        )
    except Exception as e:
        return {"success": False, "error": str(e)}

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
