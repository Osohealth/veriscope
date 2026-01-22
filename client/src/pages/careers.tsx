import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft, Briefcase, MapPin, Users, TrendingUp, Zap, Heart } from "lucide-react";

export default function Careers() {
  const jobListings = [
    {
      id: 1,
      title: "Senior Machine Learning Engineer",
      department: "Engineering",
      location: "San Francisco, CA",
      type: "Full-time",
      level: "Senior",
      description: "Build and optimize ML models for real-time price prediction and market signal detection",
      skills: ["Python", "TensorFlow", "Data Science", "AWS"]
    },
    {
      id: 2,
      title: "Maritime Data Analyst",
      department: "Analytics",
      location: "London, UK",
      type: "Full-time",
      level: "Mid-level",
      description: "Analyze vessel movements and port operations to identify market trends and opportunities",
      skills: ["SQL", "Python", "Analytics", "Excel"]
    },
    {
      id: 3,
      title: "Product Manager - Commodities",
      department: "Product",
      location: "Remote",
      type: "Full-time",
      level: "Mid-level",
      description: "Drive product vision for our commodity intelligence platform serving 10K+ traders",
      skills: ["Product Strategy", "Analytics", "Trading Knowledge", "Leadership"]
    },
    {
      id: 4,
      title: "Full Stack Engineer",
      department: "Engineering",
      location: "San Francisco, CA",
      type: "Full-time",
      level: "Mid-level",
      description: "Build scalable web applications powering real-time maritime intelligence",
      skills: ["React", "Node.js", "TypeScript", "PostgreSQL"]
    },
    {
      id: 5,
      title: "Sales Executive - EMEA",
      department: "Sales",
      location: "London, UK",
      type: "Full-time",
      level: "Mid-level",
      description: "Build relationships with enterprise clients and drive revenue growth across Europe",
      skills: ["B2B Sales", "Enterprise", "Trading Industry", "Relationship Building"]
    },
    {
      id: 6,
      title: "Data Infrastructure Engineer",
      department: "Engineering",
      location: "Remote",
      type: "Full-time",
      level: "Senior",
      description: "Design and maintain infrastructure processing 10M+ data points daily",
      skills: ["Kubernetes", "AWS", "Apache Spark", "System Design"]
    }
  ];

  const benefits = [
    { icon: Heart, title: "Health & Wellness", description: "Comprehensive health insurance, gym membership, mental health support" },
    { icon: TrendingUp, title: "Equity & Growth", description: "Competitive stock options and career development opportunities" },
    { icon: Users, title: "Diverse Team", description: "Work with talented professionals from 20+ countries" },
    { icon: Zap, title: "Flexible Work", description: "Remote-first culture with flexible hours and home office setup" },
    { icon: Briefcase, title: "PTO & Leave", description: "25 days vacation, parental leave, and sabbatical program" },
    { icon: TrendingUp, title: "Learning Budget", description: "Annual education budget for courses and conferences" }
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
            <h1 className="text-3xl font-bold mb-2">Careers at Veriscope</h1>
            <p className="text-muted-foreground">Join the team transforming global maritime and commodity trading</p>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        {/* Mission Section */}
        <section className="mb-16 max-w-4xl">
          <h2 className="text-3xl font-bold mb-6">Why Join Veriscope?</h2>
          <p className="text-lg text-muted-foreground mb-4">
            We're building the world's most comprehensive platform for maritime and commodity intelligence. If you're passionate about technology, trading, or solving complex problems with data, we want you on our team.
          </p>
          <p className="text-lg text-muted-foreground">
            Our culture is built on innovation, collaboration, and impact. You'll work alongside traders, data scientists, and engineers who are reshaping how global commerce operates.
          </p>
        </section>

        {/* Benefits Section */}
        <section className="mb-16">
          <h2 className="text-3xl font-bold mb-8">What We Offer</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {benefits.map((benefit, idx) => {
              const Icon = benefit.icon;
              return (
                <Card key={idx} className="border-border">
                  <CardHeader>
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 bg-primary/10 rounded-lg">
                        <Icon className="w-5 h-5 text-primary" />
                      </div>
                      <CardTitle className="text-lg">{benefit.title}</CardTitle>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <p className="text-sm text-muted-foreground">{benefit.description}</p>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        </section>

        {/* Open Positions */}
        <section className="mb-16">
          <h2 className="text-3xl font-bold mb-8">Open Positions</h2>
          <div className="space-y-4">
            {jobListings.map((job) => (
              <Card key={job.id} className="border-border hover:border-primary/50 transition-all cursor-pointer">
                <CardHeader>
                  <div className="flex items-start justify-between mb-2">
                    <div>
                      <CardTitle className="text-xl">{job.title}</CardTitle>
                      <p className="text-sm text-muted-foreground mt-1">{job.department}</p>
                    </div>
                    <Badge variant="secondary">{job.level}</Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <p className="text-muted-foreground">{job.description}</p>
                  
                  <div className="flex flex-wrap gap-3">
                    <div className="flex items-center gap-1 text-sm text-muted-foreground">
                      <MapPin className="w-4 h-4" />
                      {job.location}
                    </div>
                    <div className="flex items-center gap-1 text-sm text-muted-foreground">
                      <Briefcase className="w-4 h-4" />
                      {job.type}
                    </div>
                  </div>

                  <div className="flex flex-wrap gap-2 pt-2 border-t border-border">
                    {job.skills.map((skill, idx) => (
                      <Badge key={idx} variant="outline" className="text-xs">
                        {skill}
                      </Badge>
                    ))}
                  </div>

                  <Link href="/contact">
                    <Button size="sm" className="mt-2 bg-primary hover:bg-primary/90" data-testid={`button-apply-${job.id}`}>
                      Apply Now
                    </Button>
                  </Link>
                </CardContent>
              </Card>
            ))}
          </div>
        </section>

        {/* Culture Section */}
        <section className="mb-16 max-w-4xl">
          <h2 className="text-3xl font-bold mb-6">Our Culture</h2>
          <div className="space-y-4 text-muted-foreground">
            <p>
              <strong className="text-foreground">Remote-First:</strong> We embrace distributed teams and flexible work arrangements. Whether you're in San Francisco, London, or anywhere else, you can contribute meaningfully to our mission.
            </p>
            <p>
              <strong className="text-foreground">Data-Driven Decisions:</strong> We rely on data and evidence to make decisions. Your insights matter, and we value rigorous thinking over assumptions.
            </p>
            <p>
              <strong className="text-foreground">Continuous Learning:</strong> We invest in our team's growth through training, mentorship, and opportunities to work on challenging problems across different domains.
            </p>
            <p>
              <strong className="text-foreground">Impact Focused:</strong> Every role contributes directly to our mission of transforming global trade. You'll see the real-world impact of your work daily.
            </p>
          </div>
        </section>

        {/* Interview Process */}
        <section className="mb-16 bg-primary/5 border border-primary/20 rounded-lg p-8 md:p-12">
          <h2 className="text-2xl font-bold mb-8">Our Interview Process</h2>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
            {[
              { step: "1", title: "Application", description: "Submit your resume and background" },
              { step: "2", title: "Phone Screen", description: "Chat with our recruiting team" },
              { step: "3", title: "Technical/Functional", description: "Work through relevant challenges" },
              { step: "4", title: "Final Round", description: "Meet with leadership and team" }
            ].map((stage, idx) => (
              <div key={idx} className="text-center">
                <div className="w-10 h-10 bg-primary text-background rounded-full flex items-center justify-center font-bold mx-auto mb-3">
                  {stage.step}
                </div>
                <h3 className="font-semibold mb-1">{stage.title}</h3>
                <p className="text-sm text-muted-foreground">{stage.description}</p>
              </div>
            ))}
          </div>
        </section>

        {/* CTA Section */}
        <section className="bg-primary/10 border border-primary/20 rounded-lg p-8 md:p-12 text-center">
          <h2 className="text-2xl font-bold mb-3">Ready to Make an Impact?</h2>
          <p className="text-muted-foreground mb-6 max-w-2xl mx-auto">Don't see your ideal role? We're always looking for talented people. Reach out and let's explore how you can contribute to our mission.</p>
          <Link href="/contact">
            <Button size="lg" className="bg-primary hover:bg-primary/90">
              Get in Touch
            </Button>
          </Link>
        </section>
      </main>
    </div>
  );
}
