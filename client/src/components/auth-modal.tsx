import { useState } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ensureDevApiKey, markAuthenticated } from "@/lib/auth";

type AuthTab = "login" | "register";

type AuthModalProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  activeTab: AuthTab;
  onTabChange: (tab: AuthTab) => void;
  onAuthenticated: () => void;
};

type LoginFormState = {
  email: string;
  password: string;
};

type RegisterFormState = {
  firstName: string;
  lastName: string;
  email: string;
  company: string;
  phone: string;
  password: string;
  confirmPassword: string;
};

export default function AuthModal({
  open,
  onOpenChange,
  activeTab,
  onTabChange,
  onAuthenticated,
}: AuthModalProps) {
  const [loginForm, setLoginForm] = useState<LoginFormState>({ email: "", password: "" });
  const [registerForm, setRegisterForm] = useState<RegisterFormState>({
    firstName: "",
    lastName: "",
    email: "",
    company: "",
    phone: "",
    password: "",
    confirmPassword: "",
  });
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);

  const handleLogin = async () => {
    setError("");
    if (!loginForm.email || !loginForm.password) {
      setError("Email and password are required.");
      return;
    }
    setLoading(true);
    try {
      if (import.meta.env.DEV) {
        const apiKey = await ensureDevApiKey();
        markAuthenticated(apiKey ?? "vs_demo_key");
        onAuthenticated();
        onOpenChange(false);
        setLoading(false);
        return;
      }
      const response = await fetch("/v1/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: loginForm.email,
          password: loginForm.password,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        setError(data?.error || "Login failed.");
        setLoading(false);
        return;
      }
      localStorage.setItem("access_token", data.access_token);
      localStorage.setItem("refresh_token", data.refresh_token);
      localStorage.setItem("user", JSON.stringify(data.user));
      markAuthenticated();
      onAuthenticated();
      onOpenChange(false);
    } catch {
      setError("Login failed. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const handleRegister = async () => {
    setError("");
    const {
      firstName,
      lastName,
      email,
      company,
      phone,
      password,
      confirmPassword,
    } = registerForm;
    if (!firstName || !lastName || !email || !company || !phone || !password) {
      setError("All fields are required.");
      return;
    }
    if (password !== confirmPassword) {
      setError("Passwords do not match.");
      return;
    }
    setLoading(true);
    try {
      if (import.meta.env.DEV) {
        const apiKey = await ensureDevApiKey();
        markAuthenticated(apiKey ?? "vs_demo_key");
        onAuthenticated();
        onOpenChange(false);
        setLoading(false);
        return;
      }
      const registerResponse = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          firstName,
          lastName,
          email,
          company,
          phone,
          password,
        }),
      });
      const registerData = await registerResponse.json();
      if (!registerResponse.ok) {
        setError(registerData?.error || "Registration failed.");
        setLoading(false);
        return;
      }

      // Auto-login after successful registration.
      const loginResponse = await fetch("/v1/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
      });
      const loginData = await loginResponse.json();
      if (!loginResponse.ok) {
        setError(loginData?.error || "Registration succeeded, but login failed.");
        setLoading(false);
        return;
      }
      localStorage.setItem("access_token", loginData.access_token);
      localStorage.setItem("refresh_token", loginData.refresh_token);
      localStorage.setItem("user", JSON.stringify(loginData.user));
      markAuthenticated();
      onAuthenticated();
      onOpenChange(false);
    } catch {
      setError("Registration failed. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-xl border-border bg-background">
        <DialogHeader>
          <DialogTitle>Access the Platform</DialogTitle>
          <DialogDescription>
            Sign in or create an account to unlock dashboards and signals.
          </DialogDescription>
        </DialogHeader>

        <Tabs
          value={activeTab}
          onValueChange={(value) => {
            setError("");
            onTabChange(value as AuthTab);
          }}
          className="mt-2"
        >
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="login">Sign in</TabsTrigger>
            <TabsTrigger value="register">Create account</TabsTrigger>
          </TabsList>

          <TabsContent value="login" className="space-y-4">
            {error && (
              <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {error}
              </div>
            )}
            <div className="space-y-2">
              <label className="text-sm font-medium">Email</label>
              <Input
                type="email"
                value={loginForm.email}
                onChange={(e) => setLoginForm((prev) => ({ ...prev, email: e.target.value }))}
                placeholder="you@company.com"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Password</label>
              <Input
                type="password"
                value={loginForm.password}
                onChange={(e) => setLoginForm((prev) => ({ ...prev, password: e.target.value }))}
                placeholder="********"
              />
            </div>
            <Button className="w-full" onClick={handleLogin} disabled={loading}>
              {loading ? "Signing in..." : "Sign in"}
            </Button>
          </TabsContent>

          <TabsContent value="register" className="space-y-4">
            {error && (
              <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {error}
              </div>
            )}
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <label className="text-sm font-medium">First name</label>
                <Input
                  value={registerForm.firstName}
                  onChange={(e) => setRegisterForm((prev) => ({ ...prev, firstName: e.target.value }))}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Last name</label>
                <Input
                  value={registerForm.lastName}
                  onChange={(e) => setRegisterForm((prev) => ({ ...prev, lastName: e.target.value }))}
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Email</label>
              <Input
                type="email"
                value={registerForm.email}
                onChange={(e) => setRegisterForm((prev) => ({ ...prev, email: e.target.value }))}
                placeholder="you@company.com"
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <label className="text-sm font-medium">Company</label>
                <Input
                  value={registerForm.company}
                  onChange={(e) => setRegisterForm((prev) => ({ ...prev, company: e.target.value }))}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Phone</label>
                <Input
                  value={registerForm.phone}
                  onChange={(e) => setRegisterForm((prev) => ({ ...prev, phone: e.target.value }))}
                />
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="space-y-2">
                <label className="text-sm font-medium">Password</label>
                <Input
                  type="password"
                  value={registerForm.password}
                  onChange={(e) => setRegisterForm((prev) => ({ ...prev, password: e.target.value }))}
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Confirm password</label>
                <Input
                  type="password"
                  value={registerForm.confirmPassword}
                  onChange={(e) => setRegisterForm((prev) => ({ ...prev, confirmPassword: e.target.value }))}
                />
              </div>
            </div>
            <Button className="w-full" onClick={handleRegister} disabled={loading}>
              {loading ? "Creating account..." : "Create account"}
            </Button>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
