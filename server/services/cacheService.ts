interface CacheEntry<T> {
  value: T;
  expiresAt: number;
  hitCount: number;
}

interface CacheStats {
  hits: number;
  misses: number;
  entries: number;
  memoryUsage: string;
}

class CacheService {
  private cache = new Map<string, CacheEntry<any>>();
  private hits = 0;
  private misses = 0;
  private cleanupInterval: NodeJS.Timeout | null = null;

  constructor() {
    this.startCleanup();
  }

  private startCleanup() {
    this.cleanupInterval = setInterval(() => {
      this.cleanup();
    }, 60000);
  }

  private cleanup() {
    const now = Date.now();
    let removed = 0;
    const entries = Array.from(this.cache.entries());
    for (const [key, entry] of entries) {
      if (entry.expiresAt < now) {
        this.cache.delete(key);
        removed++;
      }
    }
    if (removed > 0) {
      console.log(`[Cache] Cleaned up ${removed} expired entries`);
    }
  }

  get<T>(key: string): T | undefined {
    const entry = this.cache.get(key);
    if (!entry) {
      this.misses++;
      return undefined;
    }

    if (entry.expiresAt < Date.now()) {
      this.cache.delete(key);
      this.misses++;
      return undefined;
    }

    entry.hitCount++;
    this.hits++;
    return entry.value as T;
  }

  set<T>(key: string, value: T, ttlMs: number = 30000): void {
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + ttlMs,
      hitCount: 0
    });
  }

  getOrSet<T>(key: string, fetcher: () => Promise<T>, ttlMs: number = 30000): Promise<T> {
    const cached = this.get<T>(key);
    if (cached !== undefined) {
      return Promise.resolve(cached);
    }

    return fetcher().then(value => {
      this.set(key, value, ttlMs);
      return value;
    });
  }

  invalidate(key: string): boolean {
    return this.cache.delete(key);
  }

  invalidatePattern(pattern: string): number {
    const regex = new RegExp(pattern);
    let count = 0;
    const keys = Array.from(this.cache.keys());
    for (const key of keys) {
      if (regex.test(key)) {
        this.cache.delete(key);
        count++;
      }
    }
    return count;
  }

  invalidatePrefix(prefix: string): number {
    let count = 0;
    const keys = Array.from(this.cache.keys());
    for (const key of keys) {
      if (key.startsWith(prefix)) {
        this.cache.delete(key);
        count++;
      }
    }
    return count;
  }

  clear(): void {
    this.cache.clear();
    this.hits = 0;
    this.misses = 0;
  }

  getStats(): CacheStats {
    const entries = this.cache.size;
    let totalSize = 0;
    const cacheEntries = Array.from(this.cache.entries());
    for (const [key, entry] of cacheEntries) {
      totalSize += key.length * 2;
      totalSize += JSON.stringify(entry.value).length * 2;
    }

    return {
      hits: this.hits,
      misses: this.misses,
      entries,
      memoryUsage: `${(totalSize / 1024).toFixed(2)} KB`
    };
  }

  getHitRate(): number {
    const total = this.hits + this.misses;
    return total > 0 ? this.hits / total : 0;
  }

  shutdown(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }
}

export const cacheService = new CacheService();

export const CACHE_KEYS = {
  VESSELS: 'vessels:all',
  PORTS: 'ports:all',
  COMMODITIES: 'commodities:all',
  MARKETS: 'markets:all',
  STORAGE_SITES: (portId?: string) => portId ? `storage:sites:${portId}` : 'storage:sites:all',
  ACTIVE_SIGNALS: 'signals:active',
  LATEST_PREDICTIONS: (target?: string) => target ? `predictions:latest:${target}` : 'predictions:latest:all',
  PORT_STATS: (portId: string) => `ports:stats:${portId}`,
  CRUDE_GRADES: (category?: string) => category ? `crude:grades:${category}` : 'crude:grades:all',
  REFINERIES: (region?: string) => region ? `refineries:${region}` : 'refineries:all',
  RESEARCH_REPORTS: (category?: string) => category ? `research:${category}` : 'research:all'
};

export const CACHE_TTL = {
  SHORT: 10000,
  MEDIUM: 30000,
  LONG: 60000,
  VERY_LONG: 300000,
  STATIC: 3600000
};
