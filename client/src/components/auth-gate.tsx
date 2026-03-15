import { useEffect, useMemo, useState } from "react";
import { Lock } from "lucide-react";
import { Button } from "@/components/ui/button";
import AuthModal from "@/components/auth-modal";
import { isAuthenticated as getAuthStatus } from "@/lib/auth";

type AuthGateProps = {
  children: React.ReactNode;
  title?: string;
  description?: string;
};

type AuthTab = "login" | "register";

const checkAuthenticated = () => getAuthStatus();

export default function AuthGate({
  children,
  title = "This area is locked",
  description = "Sign in or create an account to access this dashboard.",
}: AuthGateProps) {
  const [authed, setAuthed] = useState<boolean>(() => checkAuthenticated());
  const [modalOpen, setModalOpen] = useState<boolean>(() => !checkAuthenticated());
  const [activeTab, setActiveTab] = useState<AuthTab>("login");

  useEffect(() => {
    if (authed) {
      setModalOpen(false);
    }
  }, [authed]);

  const handleAuthenticated = () => {
    setAuthed(true);
  };

  const overlay = useMemo(() => {
    return (
      <div className="absolute inset-0 z-20 flex items-center justify-center bg-black/60 backdrop-blur-sm">
        <div className="mx-4 w-full max-w-md rounded-xl border border-border bg-card/95 p-6 text-center shadow-2xl">
          <div className="mx-auto mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-primary/10">
            <Lock className="h-6 w-6 text-primary" />
          </div>
          <h2 className="text-xl font-semibold">{title}</h2>
          <p className="mt-2 text-sm text-muted-foreground">{description}</p>
          <div className="mt-5 flex flex-col gap-2 sm:flex-row sm:justify-center">
            <Button
              onClick={() => {
                setActiveTab("login");
                setModalOpen(true);
              }}
            >
              Sign in
            </Button>
            <Button
              variant="outline"
              onClick={() => {
                setActiveTab("register");
                setModalOpen(true);
              }}
            >
              Create account
            </Button>
          </div>
        </div>
        <AuthModal
          open={modalOpen}
          onOpenChange={setModalOpen}
          activeTab={activeTab}
          onTabChange={setActiveTab}
          onAuthenticated={handleAuthenticated}
        />
      </div>
    );
  }, [activeTab, description, modalOpen, title]);

  if (authed) {
    return <>{children}</>;
  }

  return (
    <div className="relative min-h-screen">
      <div className="pointer-events-none select-none opacity-60 blur-[1.5px]">
        {children}
      </div>
      {overlay}
    </div>
  );
}
