import { Link, useLocation } from "wouter";
import { cn } from "@/lib/utils";

const ALERTS_NAV = [
  { label: "Activity", href: "/alerts" },
  { label: "Incidents", href: "/incidents" },
  { label: "Subscriptions", href: "/alerts/subscriptions" },
  { label: "Health", href: "/alerts/health" },
  { label: "Destinations", href: "/alerts/destinations" },
  { label: "Team", href: "/settings/team" },
  { label: "Audit", href: "/settings/audit" },
  { label: "Escalations", href: "/settings/escalations" },
];

export default function AlertsSubnav() {
  const [location] = useLocation();

  return (
    <div className="mt-4 flex flex-wrap gap-2">
      {ALERTS_NAV.map((item) => {
        const isActive = location === item.href;
        return (
          <Link key={item.href} href={item.href}>
            <a
              className={cn(
                "rounded-full border px-3 py-1 text-xs font-medium transition-colors",
                isActive
                  ? "border-primary bg-primary/15 text-primary"
                  : "border-border text-muted-foreground hover:border-primary/60 hover:text-primary"
              )}
            >
              {item.label}
            </a>
          </Link>
        );
      })}
    </div>
  );
}
