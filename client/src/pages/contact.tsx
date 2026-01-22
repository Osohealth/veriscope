import { useState } from "react";
import { Link } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ArrowLeft, Mail, Phone, MapPin, Clock } from "lucide-react";

export default function Contact() {
  const [formData, setFormData] = useState({
    name: "",
    email: "",
    company: "",
    subject: "",
    message: ""
  });
  const [submitted, setSubmitted] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // In a real app, this would send to a backend
    console.log("Contact form submitted:", formData);
    setSubmitted(true);
    setTimeout(() => {
      setFormData({ name: "", email: "", company: "", subject: "", message: "" });
      setSubmitted(false);
    }, 3000);
  };

  const contactInfo = [
    {
      icon: Mail,
      title: "Email",
      details: ["support@veriscope.com", "sales@veriscope.com"],
      description: "Email us anytime"
    },
    {
      icon: Phone,
      title: "Phone",
      details: ["+1 (415) 555-0132", "+44 (20) 7946 0958"],
      description: "Available 24/7"
    },
    {
      icon: MapPin,
      title: "Office",
      details: ["San Francisco, USA", "London, UK"],
      description: "Global presence"
    },
    {
      icon: Clock,
      title: "Hours",
      details: ["Monday - Friday: 8am - 6pm EST", "Saturday - Sunday: Closed"],
      description: "Trading week support"
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
            <h1 className="text-3xl font-bold mb-2">Contact Us</h1>
            <p className="text-muted-foreground">Get in touch with the Veriscope team</p>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-12">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Contact Form */}
          <div className="lg:col-span-2">
            <Card className="border-border">
              <CardHeader>
                <CardTitle className="text-2xl">Send us a Message</CardTitle>
                <p className="text-sm text-muted-foreground mt-2">Fill out the form below and we'll get back to you within 24 hours</p>
              </CardHeader>
              <CardContent>
                {submitted ? (
                  <div className="bg-green-500/10 border border-green-500/30 text-green-400 px-6 py-8 rounded-lg text-center">
                    <h3 className="font-semibold mb-2">Thank you for reaching out!</h3>
                    <p className="text-sm">We'll get back to you as soon as possible.</p>
                  </div>
                ) : (
                  <form onSubmit={handleSubmit} className="space-y-5">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Full Name</label>
                        <Input
                          type="text"
                          name="name"
                          value={formData.name}
                          onChange={handleChange}
                          placeholder="John Doe"
                          data-testid="input-contact-name"
                          required
                          className="bg-secondary/50 border-border"
                        />
                      </div>
                      <div className="space-y-2">
                        <label className="text-sm font-medium">Email</label>
                        <Input
                          type="email"
                          name="email"
                          value={formData.email}
                          onChange={handleChange}
                          placeholder="john@company.com"
                          data-testid="input-contact-email"
                          required
                          className="bg-secondary/50 border-border"
                        />
                      </div>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium">Company</label>
                      <Input
                        type="text"
                        name="company"
                        value={formData.company}
                        onChange={handleChange}
                        placeholder="Your Company Name"
                        data-testid="input-contact-company"
                        className="bg-secondary/50 border-border"
                      />
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium">Subject</label>
                      <Input
                        type="text"
                        name="subject"
                        value={formData.subject}
                        onChange={handleChange}
                        placeholder="How can we help?"
                        data-testid="input-contact-subject"
                        required
                        className="bg-secondary/50 border-border"
                      />
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium">Message</label>
                      <textarea
                        name="message"
                        value={formData.message}
                        onChange={handleChange}
                        placeholder="Tell us more about your inquiry..."
                        data-testid="textarea-contact-message"
                        required
                        rows={6}
                        className="w-full px-4 py-2 bg-secondary/50 border border-border rounded-lg text-foreground placeholder-muted-foreground focus:outline-none focus:border-primary resize-none"
                      />
                    </div>

                    <Button type="submit" className="w-full bg-primary hover:bg-primary/90" data-testid="button-contact-submit">
                      Send Message
                    </Button>
                  </form>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Contact Information */}
          <div className="space-y-6">
            {contactInfo.map((info, idx) => {
              const Icon = info.icon;
              return (
                <Card key={idx} className="border-border">
                  <CardHeader className="pb-3">
                    <div className="flex items-center gap-3">
                      <div className="p-2 bg-primary/10 rounded-lg">
                        <Icon className="w-5 h-5 text-primary" />
                      </div>
                      <div>
                        <CardTitle className="text-lg">{info.title}</CardTitle>
                        <p className="text-xs text-muted-foreground">{info.description}</p>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-1">
                      {info.details.map((detail, didx) => (
                        <p key={didx} className="text-sm text-muted-foreground">
                          {detail}
                        </p>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              );
            })}

            {/* FAQ Link */}
            <Card className="border-border bg-primary/5">
              <CardContent className="pt-6">
                <p className="text-sm text-muted-foreground mb-4">
                  Check out our FAQ for quick answers to common questions
                </p>
                <Button variant="outline" className="w-full" data-testid="button-contact-faq">
                  View FAQ
                </Button>
              </CardContent>
            </Card>
          </div>
        </div>

        {/* Additional Info Section */}
        <section className="mt-16 max-w-4xl">
          <h2 className="text-2xl font-bold mb-6">Getting Started is Easy</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {[
              {
                number: "1",
                title: "Tell Us About Your Needs",
                description: "Share your trading focus, data requirements, and use cases"
              },
              {
                number: "2",
                title: "Get a Custom Demo",
                description: "Our team will showcase how Veriscope fits your workflow"
              },
              {
                number: "3",
                title: "Start Trading Smarter",
                description: "Get instant access to real-time intelligence and AI predictions"
              }
            ].map((step, idx) => (
              <div key={idx} className="text-center">
                <div className="w-12 h-12 bg-primary text-background rounded-full flex items-center justify-center font-bold text-lg mx-auto mb-4">
                  {step.number}
                </div>
                <h3 className="font-semibold mb-2">{step.title}</h3>
                <p className="text-sm text-muted-foreground">{step.description}</p>
              </div>
            ))}
          </div>
        </section>

        {/* CTA Section */}
        <section className="mt-16 bg-primary/10 border border-primary/20 rounded-lg p-8 md:p-12 text-center">
          <h2 className="text-2xl font-bold mb-3">Ready to Transform Your Trading?</h2>
          <p className="text-muted-foreground mb-6 max-w-2xl mx-auto">Start your free trial and get instant access to professional-grade maritime and commodity intelligence</p>
          <Link href="/auth/register">
            <Button size="lg" className="bg-primary hover:bg-primary/90">
              Start Free Trial
            </Button>
          </Link>
        </section>
      </main>
    </div>
  );
}
