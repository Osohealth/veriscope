import { useState } from "react";
import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ArrowLeft, ChevronRight, Search } from "lucide-react";

export default function Documentation() {
  const [searchTerm, setSearchTerm] = useState("");

  const sections = [
    {
      id: "getting-started",
      title: "Getting Started",
      icon: "🚀",
      content: [
        {
          heading: "Create Your Account",
          text: "Sign up on Veriscope and complete the registration process. You'll have instant access to our platform with a free trial."
        },
        {
          heading: "Your First Dashboard",
          text: "After logging in, you'll see the Veriscope Intelligence Platform dashboard. Start by exploring the three main intelligence hubs: Commodities, Maritime, and Energy."
        },
        {
          heading: "Set Your Preferences",
          text: "Customize your dashboard by selecting the markets and metrics most relevant to your trading. You can change these anytime from the settings menu."
        }
      ]
    },
    {
      id: "commodities",
      title: "Commodities Intelligence",
      icon: "📊",
      content: [
        {
          heading: "Overview",
          text: "Access real-time data on 40+ commodities including crude oil, refined products, LNG, dry bulk, and more. Track global flows, inventories, and refinery operations."
        },
        {
          heading: "Key Modules",
          text: "Trades & Flows tracks multi-leg cargo movements. Inventories & Storage monitors storage facilities and floating storage. Freight Analytics shows ton-miles and shipping rates. Supply & Demand Balances provides global production/consumption data."
        },
        {
          heading: "Price Predictions",
          text: "Our ML models provide 1-day and 1-week price forecasts based on vessel movements, port congestion, storage changes, and market fundamentals."
        }
      ]
    },
    {
      id: "maritime",
      title: "Maritime Intelligence",
      icon: "🚢",
      content: [
        {
          heading: "Vessel Tracking",
          text: "Real-time AIS tracking of 45,000+ vessels globally. See vessel positions, speeds, destinations, and estimated arrival times. Filter by vessel type, flag, or cargo."
        },
        {
          heading: "Port Intelligence",
          text: "Monitor 500+ major ports with real-time arrival/departure tracking, waiting times, and congestion levels. Port delays are leading indicators for market movements."
        },
        {
          heading: "Container & Bunkering",
          text: "Track container operations and fuel supply events. Monitor TEU movements, operation types, and fuel prices across major hubs."
        }
      ]
    },
    {
      id: "energy",
      title: "Energy Transition",
      icon: "⚡",
      content: [
        {
          heading: "Emissions Intelligence",
          text: "Track carbon emissions from voyages and cargo. Monitor regulatory changes and renewable energy adoption. Access emissions forecasts by vessel and route."
        },
        {
          heading: "Power Markets",
          text: "Real-time power market data including supply, demand, and price curves. Track renewable energy penetration and forecast power prices."
        },
        {
          heading: "Renewable Dispatch",
          text: "Monitor renewable energy generation and dispatch patterns. Predict renewable output based on weather forecasts and capacity data."
        }
      ]
    },
    {
      id: "signals",
      title: "Signals & Alerts",
      icon: "🔔",
      content: [
        {
          heading: "Active Signals",
          text: "Veriscope monitors 1,000+ market signals including port congestion, storage anomalies, vessel delays, and supply imbalances. Get alerts when signals cross your thresholds."
        },
        {
          heading: "Creating Alerts",
          text: "Set custom alerts for specific markets, metrics, or events. Receive notifications via email, SMS, or in-app when conditions are met."
        },
        {
          heading: "Signal Analysis",
          text: "Each signal includes metadata on confidence level, historical accuracy, and recommended trading implications."
        }
      ]
    },
    {
      id: "api",
      title: "API Reference",
      icon: "⚙️",
      content: [
        {
          heading: "Authentication",
          text: "Use your API key to authenticate requests. Include the key in the Authorization header: Bearer YOUR_API_KEY"
        },
        {
          heading: "Endpoints",
          text: "GET /api/vessels - List all tracked vessels. GET /api/ports - Port statistics and data. GET /api/signals - Active market signals. GET /api/predictions - Price predictions and forecasts."
        },
        {
          heading: "Rate Limits",
          text: "Standard: 1000 requests/day. Enterprise: Unlimited. Responses include rate limit headers. Exceeding limits returns 429 Too Many Requests."
        }
      ]
    },
    {
      id: "faq",
      title: "Frequently Asked Questions",
      icon: "❓",
      content: [
        {
          heading: "How accurate are the price predictions?",
          text: "Our models achieve 94% accuracy on 1-day forecasts and 87% on 1-week forecasts based on historical backtesting. Accuracy varies by commodity and market conditions."
        },
        {
          heading: "What data sources do you use?",
          text: "We integrate AIS vessel tracking, port operations data, storage facility reports, satellite imagery, historical pricing, and weather forecasts."
        },
        {
          heading: "How real-time is the data?",
          text: "Most data updates within 30 seconds. Vessel positions refresh every 60 seconds. Storage data updates daily. Port statistics update hourly."
        },
        {
          heading: "Can I export data?",
          text: "Yes, all datasets can be exported as CSV or JSON. Use the export button on any dashboard or query the API for programmatic access."
        },
        {
          heading: "What is your uptime SLA?",
          text: "Enterprise customers have 99.9% uptime SLA. We maintain redundant systems across multiple geographic regions."
        }
      ]
    },
    {
      id: "troubleshooting",
      title: "Troubleshooting",
      icon: "🔧",
      content: [
        {
          heading: "Dashboard Not Loading",
          text: "Try clearing your browser cache and refreshing. Ensure you're using a supported browser (Chrome, Firefox, Safari, Edge). Check your internet connection."
        },
        {
          heading: "Missing Data",
          text: "Some vessels may not transmit AIS signals continuously. Storage data updates daily, so historical comparisons show best results. Check the date range selected."
        },
        {
          heading: "API Errors",
          text: "Verify your API key is valid and included in requests. Check rate limits and request format. Review the error message in the response for details."
        },
        {
          heading: "Account Issues",
          text: "Password reset available on login page. Two-factor authentication setup in settings. Contact support@veriscope.com for account access issues."
        }
      ]
    }
  ];

  const filteredSections = sections.filter(section =>
    section.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
    section.content.some(item =>
      item.heading.toLowerCase().includes(searchTerm.toLowerCase()) ||
      item.text.toLowerCase().includes(searchTerm.toLowerCase())
    )
  );

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
            <h1 className="text-3xl font-bold mb-2">Documentation</h1>
            <p className="text-muted-foreground">Everything you need to know about Veriscope</p>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        {/* Search */}
        <div className="mb-12 max-w-2xl">
          <div className="relative">
            <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 w-5 h-5 text-muted-foreground" />
            <Input
              type="text"
              placeholder="Search documentation..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              data-testid="input-docs-search"
              className="bg-secondary/50 border-border pl-10"
            />
          </div>
        </div>

        {/* Documentation Sections */}
        <div className="space-y-8">
          {filteredSections.length > 0 ? (
            filteredSections.map((section) => (
              <section key={section.id} id={section.id} className="scroll-mt-20">
                <div className="flex items-center gap-3 mb-6">
                  <span className="text-4xl">{section.icon}</span>
                  <h2 className="text-3xl font-bold">{section.title}</h2>
                </div>

                <div className="space-y-4">
                  {section.content.map((item, idx) => (
                    <Card key={idx} className="border-border">
                      <CardHeader>
                        <CardTitle className="text-lg">{item.heading}</CardTitle>
                      </CardHeader>
                      <CardContent>
                        <p className="text-muted-foreground leading-relaxed">{item.text}</p>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </section>
            ))
          ) : (
            <div className="text-center py-12">
              <p className="text-muted-foreground mb-4">No results found for "{searchTerm}"</p>
              <Button
                variant="outline"
                onClick={() => setSearchTerm("")}
                data-testid="button-clear-search"
              >
                Clear Search
              </Button>
            </div>
          )}
        </div>

        {/* Table of Contents */}
        {searchTerm === "" && (
          <aside className="mt-16 bg-secondary/30 border border-border rounded-lg p-6">
            <h3 className="font-semibold mb-4 text-lg">Quick Navigation</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {sections.map((section) => (
                <a
                  key={section.id}
                  href={`#${section.id}`}
                  className="flex items-center gap-2 text-primary hover:text-primary/80 transition-colors p-2 rounded hover:bg-primary/5"
                  data-testid={`link-doc-${section.id}`}
                >
                  <span>{section.icon}</span>
                  <span>{section.title}</span>
                  <ChevronRight className="w-4 h-4 ml-auto" />
                </a>
              ))}
            </div>
          </aside>
        )}

        {/* Support CTA */}
        <section className="mt-16 bg-primary/10 border border-primary/20 rounded-lg p-8 md:p-12 text-center">
          <h2 className="text-2xl font-bold mb-3">Need More Help?</h2>
          <p className="text-muted-foreground mb-6 max-w-2xl mx-auto">Can't find what you're looking for? Our support team is here to help.</p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link href="/contact">
              <Button className="bg-primary hover:bg-primary/90">
                Contact Support
              </Button>
            </Link>
            <Link href="/blog">
              <Button variant="outline">
                Read Our Blog
              </Button>
            </Link>
          </div>
        </section>
      </main>
    </div>
  );
}
