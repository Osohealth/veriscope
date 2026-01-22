import { useState, useEffect } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Ship, Anchor, Bell, Download, BarChart3, Map, ArrowRight, CheckCircle2 } from "lucide-react";

const ONBOARDING_STEPS = [
  {
    id: "welcome",
    title: "Welcome to Veriscope",
    description: "Your comprehensive maritime intelligence platform for real-time vessel tracking, port monitoring, and market analytics.",
    icon: Ship,
    features: [
      "Real-time AIS vessel tracking",
      "Port congestion monitoring",
      "AI-powered price predictions",
      "Interactive analytics dashboards"
    ]
  },
  {
    id: "navigation",
    title: "Navigate the Platform",
    description: "Use the sidebar to access different modules and intelligence hubs.",
    icon: Map,
    features: [
      "Dashboard: Overview of key metrics",
      "Maritime: Vessel tracking and port events",
      "Commodities: Market data and analysis",
      "Signals: Real-time alerts and predictions"
    ]
  },
  {
    id: "watchlists",
    title: "Create Watchlists",
    description: "Track specific vessels, ports, and commodities that matter to your business.",
    icon: Bell,
    features: [
      "Monitor vessel movements",
      "Track port congestion",
      "Follow commodity prices",
      "Receive custom alerts"
    ]
  },
  {
    id: "exports",
    title: "Export Your Data",
    description: "Download data in CSV format for further analysis in your preferred tools.",
    icon: Download,
    features: [
      "Export vessel databases",
      "Download port information",
      "Save market predictions",
      "Analyze trends offline"
    ]
  },
  {
    id: "analytics",
    title: "Explore Analytics",
    description: "Dive deep into market trends with interactive charts and visualizations.",
    icon: BarChart3,
    features: [
      "Price movement analysis",
      "Congestion trends",
      "Trade flow patterns",
      "Predictive insights"
    ]
  }
];

interface OnboardingModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export function OnboardingModal({ isOpen, onClose }: OnboardingModalProps) {
  const [currentStep, setCurrentStep] = useState(0);
  const [completedSteps, setCompletedSteps] = useState<number[]>([]);

  const step = ONBOARDING_STEPS[currentStep];
  const IconComponent = step.icon;
  const isLastStep = currentStep === ONBOARDING_STEPS.length - 1;

  const handleNext = () => {
    setCompletedSteps((prev) => [...prev, currentStep]);
    if (isLastStep) {
      localStorage.setItem("veriscope_onboarding_complete", "true");
      onClose();
    } else {
      setCurrentStep((prev) => prev + 1);
    }
  };

  const handlePrevious = () => {
    if (currentStep > 0) {
      setCurrentStep((prev) => prev - 1);
    }
  };

  const handleSkip = () => {
    localStorage.setItem("veriscope_onboarding_complete", "true");
    onClose();
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => !open && handleSkip()}>
      <DialogContent className="bg-slate-900 border-slate-700 max-w-lg">
        <DialogHeader>
          <div className="flex items-center gap-3 mb-4">
            <div className="p-3 bg-blue-900/50 rounded-xl text-blue-400">
              <IconComponent className="h-6 w-6" />
            </div>
            <div>
              <Badge variant="outline" className="border-blue-700 text-blue-400 mb-1">
                Step {currentStep + 1} of {ONBOARDING_STEPS.length}
              </Badge>
              <DialogTitle className="text-white text-xl">{step.title}</DialogTitle>
            </div>
          </div>
          <DialogDescription className="text-slate-400">
            {step.description}
          </DialogDescription>
        </DialogHeader>

        <div className="py-4">
          <ul className="space-y-3">
            {step.features.map((feature, index) => (
              <li key={index} className="flex items-start gap-3 text-slate-300">
                <CheckCircle2 className="h-5 w-5 text-green-500 mt-0.5 flex-shrink-0" />
                <span>{feature}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="flex gap-1 justify-center mb-4">
          {ONBOARDING_STEPS.map((_, index) => (
            <div
              key={index}
              className={`h-1.5 w-8 rounded-full transition-colors ${
                index === currentStep
                  ? "bg-blue-500"
                  : completedSteps.includes(index)
                  ? "bg-green-500"
                  : "bg-slate-700"
              }`}
            />
          ))}
        </div>

        <DialogFooter className="flex-col sm:flex-row gap-2">
          <Button
            variant="ghost"
            onClick={handleSkip}
            className="text-slate-400 hover:text-white"
            data-testid="button-skip-onboarding"
          >
            Skip Tour
          </Button>
          <div className="flex gap-2">
            {currentStep > 0 && (
              <Button
                variant="outline"
                onClick={handlePrevious}
                className="border-slate-700"
                data-testid="button-previous-step"
              >
                Previous
              </Button>
            )}
            <Button
              onClick={handleNext}
              className="bg-blue-600 hover:bg-blue-700"
              data-testid="button-next-step"
            >
              {isLastStep ? (
                "Get Started"
              ) : (
                <>
                  Next
                  <ArrowRight className="h-4 w-4 ml-2" />
                </>
              )}
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export function useOnboarding() {
  const [showOnboarding, setShowOnboarding] = useState(false);

  useEffect(() => {
    const completed = localStorage.getItem("veriscope_onboarding_complete");
    if (!completed) {
      const timer = setTimeout(() => setShowOnboarding(true), 1000);
      return () => clearTimeout(timer);
    }
  }, []);

  return {
    showOnboarding,
    openOnboarding: () => setShowOnboarding(true),
    closeOnboarding: () => setShowOnboarding(false),
    resetOnboarding: () => {
      localStorage.removeItem("veriscope_onboarding_complete");
      setShowOnboarding(true);
    }
  };
}
