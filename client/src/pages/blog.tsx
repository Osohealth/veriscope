import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Calendar, User, ArrowRight } from "lucide-react";

const blogPosts = [
  {
    id: 1,
    slug: "ai-powered-maritime-predictions",
    title: "How AI is Revolutionizing Maritime Predictions",
    excerpt: "Discover how machine learning models are transforming vessel tracking and cargo movement predictions with unprecedented accuracy.",
    content: "Machine learning has become the cornerstone of modern maritime intelligence. By analyzing vessel movements, port congestion, and historical patterns, AI models can now predict cargo arrivals with 94% accuracy, enabling traders to make informed decisions weeks in advance. Veriscope's proprietary ML engine processes over 10 million data points daily to identify market opportunities before they appear in traditional analytics.",
    date: "November 28, 2024",
    author: "Sarah Johnson",
    category: "Maritime Intelligence",
    readTime: "8 min read",
    image: "📊"
  },
  {
    id: 2,
    slug: "energy-transition-opportunities",
    title: "Energy Transition: Opportunities in Renewable Trading",
    excerpt: "Explore how renewable energy markets are creating new trading opportunities for commodity professionals.",
    content: "The global energy transition is generating unprecedented opportunities for traders willing to adapt their strategies. Renewable energy production is becoming increasingly predictable through weather modeling and AI analysis. Smart traders are leveraging this data to position themselves in power markets before capacity-constrained grids activate backup generation. Veriscope's energy transition module provides real-time power market data and renewable dispatch forecasts.",
    date: "November 25, 2024",
    author: "Michael Chen",
    category: "Energy Markets",
    readTime: "10 min read",
    image: "⚡"
  },
  {
    id: 3,
    slug: "port-delays-market-impact",
    title: "Understanding Port Delays and Their Market Impact",
    excerpt: "Learn how bottlenecks at major ports create ripple effects across global commodity markets.",
    content: "Port delays are more than operational headaches—they're market signals. When Rotterdam experiences delays, it affects crude oil spreads, shipping rates, and storage utilization across Europe. By monitoring real-time port congestion and vessel waiting times, traders can anticipate price movements before they happen. Our research shows that traders using port delay data as a leading indicator outperform the market by 3-5% annually.",
    date: "November 22, 2024",
    author: "Emma Williams",
    category: "Port Intelligence",
    readTime: "7 min read",
    image: "🚢"
  },
  {
    id: 4,
    slug: "storage-anomalies-signals",
    title: "Identifying Storage Anomalies: The Forgotten Signal",
    excerpt: "Storage changes often predict price movements better than traditional indicators. Here's why.",
    content: "While most traders focus on production and demand data, savvy professionals know that storage movements are powerful predictive signals. When storage levels at key hubs spike unexpectedly, it often signals upcoming price weakness. Conversely, rapid inventory drawdowns can precede rallies. Veriscope's storage intelligence module monitors 500+ major storage facilities globally, identifying anomalies that signal market turning points.",
    date: "November 20, 2024",
    author: "David Martinez",
    category: "Commodity Intelligence",
    readTime: "6 min read",
    image: "📦"
  },
  {
    id: 5,
    slug: "real-time-ais-advantage",
    title: "The Real-Time AIS Advantage: Trading with Live Vessel Data",
    excerpt: "Real-time vessel tracking is no longer a luxury—it's essential for competitive advantage.",
    content: "Access to real-time AIS (Automatic Identification System) data transforms trading strategy. You can now see exactly where cargoes are, which vessels are moving to specific destinations, and when they'll arrive at their next port. This information window—often 2-3 weeks—allows traders to position before market consensus catches up. The difference between reactive and predictive trading comes down to data access and interpretation.",
    date: "November 18, 2024",
    author: "Jessica Thompson",
    category: "Maritime Technology",
    readTime: "9 min read",
    image: "🌍"
  },
  {
    id: 6,
    slug: "commodity-flows-geopolitics",
    title: "Mapping Commodity Flows in a Changing Geopolitical Landscape",
    excerpt: "How trade route changes and geopolitical events reshape commodity movement patterns.",
    content: "Recent geopolitical events have fundamentally altered commodity flow patterns. The Suez Canal situation, sanctions regimes, and trade agreements are redirecting thousands of vessels to longer alternative routes. Traders who understand these route dynamics can identify price opportunities across different markets. Veriscope tracks these changes in real-time, showing you exactly how geopolitics translates into market opportunities.",
    date: "November 15, 2024",
    author: "Robert Kim",
    category: "Market Analysis",
    readTime: "11 min read",
    image: "🗺️"
  }
];

export default function Blog() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Header */}
      <header className="border-b border-border bg-background/95 backdrop-blur sticky top-0 z-50 py-4">
        <div className="container mx-auto px-4">
          <Link href="/" className="flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors w-fit mb-4">
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </Link>
          <div>
            <h1 className="text-3xl font-bold mb-2">Veriscope Blog</h1>
            <p className="text-muted-foreground">Insights on maritime intelligence, commodity trading, and energy markets</p>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        {/* Featured Post */}
        <div className="mb-16">
          <h2 className="text-2xl font-bold mb-6">Featured</h2>
          <Link href={`/blog/${blogPosts[0].slug}`}>
            <Card className="border-border hover:border-primary/50 transition-all cursor-pointer overflow-hidden">
              <div className="grid grid-cols-1 md:grid-cols-3">
                <div className="h-64 md:h-auto bg-primary/10 flex items-center justify-center text-6xl">
                  {blogPosts[0].image}
                </div>
                <CardContent className="md:col-span-2 p-8 flex flex-col justify-between">
                  <div>
                    <div className="flex gap-2 mb-4">
                      <Badge variant="secondary">{blogPosts[0].category}</Badge>
                      <Badge variant="outline">{blogPosts[0].readTime}</Badge>
                    </div>
                    <h3 className="text-2xl font-bold mb-3">{blogPosts[0].title}</h3>
                    <p className="text-muted-foreground mb-4">{blogPosts[0].excerpt}</p>
                  </div>
                  <div className="flex items-center justify-between pt-4 border-t border-border">
                    <div className="flex items-center gap-4 text-sm text-muted-foreground">
                      <div className="flex items-center gap-1">
                        <Calendar className="w-4 h-4" />
                        {blogPosts[0].date}
                      </div>
                      <div className="flex items-center gap-1">
                        <User className="w-4 h-4" />
                        {blogPosts[0].author}
                      </div>
                    </div>
                    <ArrowRight className="w-5 h-5 text-primary" />
                  </div>
                </CardContent>
              </div>
            </Card>
          </Link>
        </div>

        {/* Recent Posts Grid */}
        <div>
          <h2 className="text-2xl font-bold mb-6">Recent Articles</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {blogPosts.slice(1).map((post) => (
              <Link key={post.id} href={`/blog/${post.slug}`}>
                <Card className="border-border hover:border-primary/50 transition-all cursor-pointer overflow-hidden h-full flex flex-col">
                  <div className="h-40 bg-primary/10 flex items-center justify-center text-5xl">
                    {post.image}
                  </div>
                  <CardHeader className="flex-1">
                    <div className="flex gap-2 mb-3">
                      <Badge variant="secondary" className="text-xs">{post.category}</Badge>
                    </div>
                    <CardTitle className="text-lg line-clamp-2">{post.title}</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <p className="text-sm text-muted-foreground line-clamp-2">{post.excerpt}</p>
                    <div className="flex items-center justify-between pt-4 border-t border-border">
                      <div className="flex flex-col gap-1 text-xs text-muted-foreground">
                        <span>{post.date}</span>
                        <span>{post.readTime}</span>
                      </div>
                      <ArrowRight className="w-4 h-4 text-primary" />
                    </div>
                  </CardContent>
                </Card>
              </Link>
            ))}
          </div>
        </div>

        {/* Newsletter Signup */}
        <div className="mt-20 bg-primary/5 border border-primary/20 rounded-lg p-8 md:p-12 text-center">
          <h3 className="text-2xl font-bold mb-2">Stay Updated</h3>
          <p className="text-muted-foreground mb-6">Get weekly insights on maritime intelligence and commodity trading trends</p>
          <div className="flex gap-2 max-w-md mx-auto">
            <input
              type="email"
              placeholder="Enter your email"
              className="flex-1 px-4 py-2 bg-background border border-border rounded-lg text-foreground placeholder-muted-foreground focus:outline-none focus:border-primary"
            />
            <Button className="bg-primary hover:bg-primary/90">Subscribe</Button>
          </div>
        </div>
      </main>
    </div>
  );
}
