import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Link } from "wouter";
import { ArrowLeft, Mail, Lock, User, Building2, Phone, CheckCircle, Zap, BarChart3, Globe } from "lucide-react";

export default function Register() {
  const [formData, setFormData] = useState({
    firstName: "",
    lastName: "",
    email: "",
    company: "",
    phone: "",
    password: "",
    confirmPassword: "",
    agreeTerms: false,
  });
  const [error, setError] = useState("");
  const [success, setSuccess] = useState(false);
  const [currentStep, setCurrentStep] = useState(1);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value, type, checked } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === "checkbox" ? checked : value
    }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");

    if (!formData.firstName || !formData.lastName || !formData.email || !formData.company || !formData.phone || !formData.password) {
      setError("All fields are required");
      return;
    }

    if (formData.password !== formData.confirmPassword) {
      setError("Passwords do not match");
      return;
    }

    if (!formData.agreeTerms) {
      setError("You must agree to the terms and conditions");
      return;
    }

    try {
      const response = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          firstName: formData.firstName,
          lastName: formData.lastName,
          email: formData.email,
          company: formData.company,
          phone: formData.phone,
          password: formData.password,
        })
      });

      if (!response.ok) {
        const data = await response.json();
        setError(data.error || "Registration failed");
        return;
      }

      setSuccess(true);
      setTimeout(() => {
        window.location.href = "/";
      }, 2000);
    } catch (err) {
      setError("An error occurred. Please try again.");
    }
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Header */}
      <header className="border-b border-border bg-background/95 backdrop-blur sticky top-0 z-50 py-4">
        <div className="container mx-auto px-4">
          <Link href="/" className="flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors w-fit">
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </Link>
        </div>
      </header>

      {/* Main Content */}
      <div className="min-h-[calc(100vh-73px)] flex items-center justify-center py-12 px-4">
        <div className="w-full max-w-5xl">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 items-center">
            {/* Left Side - Benefits */}
            <div className="space-y-8 hidden lg:block">
              <div>
                <h1 className="text-5xl font-bold mb-4 leading-tight">
                  Join Veriscope
                </h1>
                <p className="text-xl text-muted-foreground">
                  Access enterprise-grade maritime and commodity intelligence
                </p>
              </div>

              <div className="space-y-4">
                {[
                  {
                    icon: Globe,
                    title: "Global Coverage",
                    description: "Real-time tracking of 45,000+ vessels across 500+ ports"
                  },
                  {
                    icon: BarChart3,
                    title: "Advanced Analytics",
                    description: "AI-powered predictions and market intelligence"
                  },
                  {
                    icon: Zap,
                    title: "Instant Alerts",
                    description: "24/7 monitoring with intelligent signal notifications"
                  }
                ].map((benefit, idx) => {
                  const Icon = benefit.icon;
                  return (
                    <div key={idx} className="flex gap-4">
                      <div className="flex-shrink-0">
                        <div className="flex items-center justify-center h-12 w-12 rounded-lg bg-primary/10">
                          <Icon className="h-6 w-6 text-primary" />
                        </div>
                      </div>
                      <div>
                        <h3 className="font-semibold text-foreground">{benefit.title}</h3>
                        <p className="text-sm text-muted-foreground">{benefit.description}</p>
                      </div>
                    </div>
                  );
                })}
              </div>

              <div className="pt-8 border-t border-border">
                <p className="text-sm text-muted-foreground mb-4">Trusted by leading maritime & commodity traders</p>
                <div className="flex gap-2">
                  <div className="h-8 w-8 rounded-full bg-primary/20 flex items-center justify-center text-xs font-semibold text-primary">10K+</div>
                  <div className="h-8 w-8 rounded-full bg-primary/20 flex items-center justify-center text-xs font-semibold text-primary">180</div>
                  <div className="h-8 w-8 rounded-full bg-primary/20 flex items-center justify-center text-xs font-semibold text-primary">4.9</div>
                </div>
              </div>
            </div>

            {/* Right Side - Form */}
            <div>
              <Card className="border-border shadow-2xl">
                <CardContent className="p-8">
                  {success ? (
                    <div className="text-center space-y-4">
                      <div className="flex justify-center">
                        <div className="rounded-full bg-green-500/20 p-3">
                          <CheckCircle className="w-8 h-8 text-green-400" />
                        </div>
                      </div>
                      <div>
                        <h3 className="text-lg font-semibold text-foreground mb-1">Welcome to Veriscope!</h3>
                        <p className="text-sm text-muted-foreground">Your account has been created successfully. Redirecting to dashboard...</p>
                      </div>
                    </div>
                  ) : (
                    <form onSubmit={handleSubmit} className="space-y-5">
                      <div>
                        <h2 className="text-2xl font-bold mb-1">Create Account</h2>
                        <p className="text-sm text-muted-foreground">Step {currentStep} of 2</p>
                      </div>

                      {/* Progress Bar */}
                      <div className="flex gap-2">
                        <div className={`h-1 flex-1 rounded-full transition-all ${currentStep >= 1 ? 'bg-primary' : 'bg-secondary'}`}></div>
                        <div className={`h-1 flex-1 rounded-full transition-all ${currentStep >= 2 ? 'bg-primary' : 'bg-secondary'}`}></div>
                      </div>

                      {error && (
                        <div className="bg-red-500/10 border border-red-500/30 text-red-400 px-4 py-3 rounded-lg text-sm flex gap-2">
                          <span>⚠️</span>
                          <span>{error}</span>
                        </div>
                      )}

                      {currentStep === 1 ? (
                        <div className="space-y-4">
                          <div className="grid grid-cols-2 gap-3">
                            <div className="space-y-2">
                              <label className="text-sm font-medium text-foreground flex items-center gap-2">
                                <User className="w-4 h-4 text-primary" />
                                First Name
                              </label>
                              <Input
                                type="text"
                                name="firstName"
                                value={formData.firstName}
                                onChange={handleChange}
                                placeholder="John"
                                data-testid="input-firstName"
                                className="bg-secondary/50 border-border"
                              />
                            </div>
                            <div className="space-y-2">
                              <label className="text-sm font-medium text-foreground flex items-center gap-2">
                                <User className="w-4 h-4 text-primary" />
                                Last Name
                              </label>
                              <Input
                                type="text"
                                name="lastName"
                                value={formData.lastName}
                                onChange={handleChange}
                                placeholder="Doe"
                                data-testid="input-lastName"
                                className="bg-secondary/50 border-border"
                              />
                            </div>
                          </div>

                          <div className="space-y-2">
                            <label className="text-sm font-medium text-foreground flex items-center gap-2">
                              <Mail className="w-4 h-4 text-primary" />
                              Email Address
                            </label>
                            <Input
                              type="email"
                              name="email"
                              value={formData.email}
                              onChange={handleChange}
                              placeholder="john@company.com"
                              data-testid="input-email"
                              className="bg-secondary/50 border-border"
                            />
                          </div>

                          <div className="space-y-2">
                            <label className="text-sm font-medium text-foreground flex items-center gap-2">
                              <Building2 className="w-4 h-4 text-primary" />
                              Company
                            </label>
                            <Input
                              type="text"
                              name="company"
                              value={formData.company}
                              onChange={handleChange}
                              placeholder="Your Company"
                              data-testid="input-company"
                              className="bg-secondary/50 border-border"
                            />
                          </div>

                          <div className="space-y-2">
                            <label className="text-sm font-medium text-foreground flex items-center gap-2">
                              <Phone className="w-4 h-4 text-primary" />
                              Phone Number
                            </label>
                            <Input
                              type="tel"
                              name="phone"
                              value={formData.phone}
                              onChange={handleChange}
                              placeholder="+1 (555) 123-4567"
                              data-testid="input-phone"
                              className="bg-secondary/50 border-border"
                            />
                          </div>

                          <Button 
                            type="button"
                            onClick={() => setCurrentStep(2)}
                            className="w-full bg-primary hover:bg-primary/90"
                            data-testid="button-next"
                          >
                            Continue
                          </Button>
                        </div>
                      ) : (
                        <div className="space-y-4">
                          <div className="space-y-2">
                            <label className="text-sm font-medium text-foreground flex items-center gap-2">
                              <Lock className="w-4 h-4 text-primary" />
                              Password
                            </label>
                            <Input
                              type="password"
                              name="password"
                              value={formData.password}
                              onChange={handleChange}
                              placeholder="••••••••"
                              data-testid="input-password"
                              className="bg-secondary/50 border-border"
                            />
                            <p className="text-xs text-muted-foreground">Minimum 8 characters with uppercase and numbers</p>
                          </div>

                          <div className="space-y-2">
                            <label className="text-sm font-medium text-foreground flex items-center gap-2">
                              <Lock className="w-4 h-4 text-primary" />
                              Confirm Password
                            </label>
                            <Input
                              type="password"
                              name="confirmPassword"
                              value={formData.confirmPassword}
                              onChange={handleChange}
                              placeholder="••••••••"
                              data-testid="input-confirmPassword"
                              className="bg-secondary/50 border-border"
                            />
                          </div>

                          <div className="flex items-start space-x-3 pt-2">
                            <Checkbox
                              id="terms"
                              name="agreeTerms"
                              checked={formData.agreeTerms}
                              onCheckedChange={(checked) =>
                                setFormData(prev => ({ ...prev, agreeTerms: checked as boolean }))
                              }
                              data-testid="checkbox-terms"
                              className="mt-1"
                            />
                            <label htmlFor="terms" className="text-sm text-muted-foreground cursor-pointer">
                              I agree to the{" "}
                              <span className="text-primary hover:underline">terms and conditions</span>
                              {" "}and{" "}
                              <span className="text-primary hover:underline">privacy policy</span>
                            </label>
                          </div>

                          <div className="flex gap-3">
                            <Button 
                              type="button"
                              onClick={() => setCurrentStep(1)}
                              variant="outline"
                              className="flex-1"
                              data-testid="button-back"
                            >
                              Back
                            </Button>
                            <Button 
                              type="submit"
                              className="flex-1 bg-primary hover:bg-primary/90"
                              data-testid="button-register"
                            >
                              Create Account
                            </Button>
                          </div>
                        </div>
                      )}

                      <div className="text-center text-sm text-muted-foreground pt-2">
                        Already have an account?{" "}
                        <Link href="/auth/login" className="text-primary hover:underline font-medium">
                          Sign in
                        </Link>
                      </div>
                    </form>
                  )}
                </CardContent>
              </Card>

              {/* Mobile Benefits */}
              <div className="lg:hidden mt-8 space-y-4">
                <div className="flex gap-3">
                  <Globe className="w-5 h-5 text-primary flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="font-semibold text-sm">Global Coverage</p>
                    <p className="text-xs text-muted-foreground">45,000+ vessels tracked</p>
                  </div>
                </div>
                <div className="flex gap-3">
                  <BarChart3 className="w-5 h-5 text-primary flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="font-semibold text-sm">Advanced Analytics</p>
                    <p className="text-xs text-muted-foreground">AI-powered insights</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
