import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ArrowLeft, Users, Zap, Globe, Award } from "lucide-react";

export default function About() {
  const team = [
    { name: "Sarah Johnson", role: "CEO & Co-founder", expertise: "Maritime Intelligence" },
    { name: "Michael Chen", role: "CTO & Co-founder", expertise: "Machine Learning & Data Science" },
    { name: "Emma Williams", role: "Head of Commodities", expertise: "Energy & Commodity Trading" },
    { name: "David Martinez", role: "Head of Product", expertise: "Platform Architecture" },
    { name: "Jessica Thompson", role: "VP Analytics", expertise: "Predictive Modeling" },
    { name: "Robert Kim", role: "Head of Partnerships", expertise: "Enterprise Relations" }
  ];

  const values = [
    {
      icon: Zap,
      title: "Innovation",
      description: "Continuously pushing the boundaries of what's possible in maritime and commodity intelligence"
    },
    {
      icon: Globe,
      title: "Global Impact",
      description: "Empowering traders worldwide to make better decisions with real-time, actionable intelligence"
    },
    {
      icon: Award,
      title: "Excellence",
      description: "Committed to the highest standards of data accuracy, analysis, and service quality"
    },
    {
      icon: Users,
      title: "Collaboration",
      description: "Building partnerships with leading institutions to advance the industry"
    }
  ];

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
            <h1 className="text-3xl font-bold mb-2">About Veriscope</h1>
            <p className="text-muted-foreground">Transforming global trade with AI-powered intelligence</p>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        {/* Mission Section */}
        <section className="mb-16 max-w-4xl">
          <h2 className="text-3xl font-bold mb-6">Our Mission</h2>
          <p className="text-lg text-muted-foreground mb-4">
            Veriscope is building the world's most comprehensive platform for maritime and commodity intelligence. We combine real-time vessel tracking, advanced analytics, and machine learning to help traders, logistics professionals, and energy companies make better decisions faster.
          </p>
          <p className="text-lg text-muted-foreground">
            By making professional-grade market intelligence accessible to forward-thinking organizations, we're transforming how global trade operates. Our mission is to reduce information asymmetry, enable smarter decision-making, and create competitive advantage for our users.
          </p>
        </section>

        {/* Story Section */}
        <section className="mb-16 max-w-4xl">
          <h2 className="text-3xl font-bold mb-6">Our Story</h2>
          <p className="text-muted-foreground mb-4">
            Veriscope was founded in 2023 by a team of ex-traders, data scientists, and maritime professionals who saw an opportunity to democratize market intelligence. We recognized that while satellite imagery, vessel tracking data, and trading data existed, they weren't integrated into a unified platform that traders could actually use.
          </p>
          <p className="text-muted-foreground mb-4">
            Our founders spent years managing these data streams manually, losing competitive edge to faster, better-connected competitors. They realized the gap wasn't a technology problem—it was an integration problem. By bringing together real-time vessel movements, port operations, storage data, and market signals, we could create something genuinely transformative.
          </p>
          <p className="text-muted-foreground">
            Today, Veriscope serves over 10,000 traders and logistics professionals across 50+ countries, processing over 10 million data points daily to identify market opportunities and risks.
          </p>
        </section>

        {/* Values Section */}
        <section className="mb-16">
          <h2 className="text-3xl font-bold mb-8">Our Values</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {values.map((value, idx) => {
              const Icon = value.icon;
              return (
                <Card key={idx} className="border-border">
                  <CardHeader>
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 bg-primary/10 rounded-lg">
                        <Icon className="w-6 h-6 text-primary" />
                      </div>
                      <CardTitle>{value.title}</CardTitle>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <p className="text-muted-foreground text-sm">{value.description}</p>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        </section>

        {/* Team Section */}
        <section className="mb-16">
          <h2 className="text-3xl font-bold mb-8">Leadership Team</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {team.map((member, idx) => (
              <Card key={idx} className="border-border">
                <CardHeader>
                  <div className="w-12 h-12 bg-primary/20 rounded-full flex items-center justify-center mb-4">
                    <Users className="w-6 h-6 text-primary" />
                  </div>
                  <CardTitle className="text-lg">{member.name}</CardTitle>
                  <p className="text-sm text-primary font-medium">{member.role}</p>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground">{member.expertise}</p>
                </CardContent>
              </Card>
            ))}
          </div>
        </section>

        {/* Achievements Section */}
        <section className="mb-16 bg-primary/5 border border-primary/20 rounded-lg p-8 md:p-12">
          <h2 className="text-3xl font-bold mb-8">Our Impact</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            {[
              { number: "10K+", label: "Active Traders" },
              { number: "50+", label: "Countries" },
              { number: "10M+", label: "Data Points/Day" },
              { number: "94%", label: "Prediction Accuracy" }
            ].map((stat, idx) => (
              <div key={idx} className="text-center">
                <div className="text-3xl md:text-4xl font-bold text-primary mb-2">{stat.number}</div>
                <p className="text-sm text-muted-foreground">{stat.label}</p>
              </div>
            ))}
          </div>
        </section>

        {/* Technology Section */}
        <section className="mb-16 max-w-4xl">
          <h2 className="text-3xl font-bold mb-6">Our Technology</h2>
          <p className="text-muted-foreground mb-4">
            Veriscope's platform is built on a foundation of cutting-edge technology:
          </p>
          <ul className="space-y-3 text-muted-foreground">
            <li className="flex gap-3">
              <span className="text-primary font-bold">•</span>
              <span><strong>Real-Time AIS Integration:</strong> Live vessel tracking from 45,000+ vessels globally</span>
            </li>
            <li className="flex gap-3">
              <span className="text-primary font-bold">•</span>
              <span><strong>Machine Learning Models:</strong> Proprietary ML algorithms for price prediction and pattern recognition</span>
            </li>
            <li className="flex gap-3">
              <span className="text-primary font-bold">•</span>
              <span><strong>Port Intelligence:</strong> Real-time monitoring of 500+ major ports with delay tracking</span>
            </li>
            <li className="flex gap-3">
              <span className="text-primary font-bold">•</span>
              <span><strong>Storage Monitoring:</strong> Comprehensive tracking of storage facilities across key markets</span>
            </li>
            <li className="flex gap-3">
              <span className="text-primary font-bold">•</span>
              <span><strong>WebSocket Architecture:</strong> Sub-second latency for real-time alerts and updates</span>
            </li>
          </ul>
        </section>

        {/* CTA Section */}
        <section className="bg-primary/10 border border-primary/20 rounded-lg p-8 md:p-12 text-center">
          <h2 className="text-2xl font-bold mb-3">Ready to Join Our Community?</h2>
          <p className="text-muted-foreground mb-6 max-w-2xl mx-auto">Start leveraging AI-powered maritime intelligence to transform your trading strategy</p>
          <Link href="/auth/register">
            <Button size="lg" className="bg-primary hover:bg-primary/90">
              Get Started Today
            </Button>
          </Link>
        </section>
      </main>
    </div>
  );
}
