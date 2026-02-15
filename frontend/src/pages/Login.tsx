import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Activity, ArrowLeft, LogIn, Shield, User } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

const Login = () => {
    const navigate = useNavigate();
    const { login } = useAuth();
    const { toast } = useToast();
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [isLoading, setIsLoading] = useState(false);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setIsLoading(true);

        try {
            const success = await login(username, password);
            if (success) {
                toast({
                    title: "Welcome back!",
                    description: "Successfully logged in.",
                });
                navigate("/dashboard");
            } else {
                toast({
                    title: "Login failed",
                    description: "Please check your credentials and try again.",
                    variant: "destructive",
                });
            }
        } catch (error) {
            toast({
                title: "Error",
                description: "An error occurred during login.",
                variant: "destructive",
            });
        } finally {
            setIsLoading(false);
        }
    };

    const handleQuickLogin = async (username: string, password: string, isAdmin: boolean) => {
        setIsLoading(true);

        try {
            const success = await login(username, password);
            if (success) {
                toast({
                    title: `Welcome${isAdmin ? ' Admin' : ''}!`,
                    description: `Logged in as ${isAdmin ? 'administrator' : 'demo user'}.`,
                });
                navigate("/dashboard");
            }
        } catch (error) {
            toast({
                title: "Error",
                description: "An error occurred during login.",
                variant: "destructive",
            });
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="min-h-screen bg-background overflow-hidden flex items-center justify-center">
            {/* Ambient glow background */}
            <div className="fixed inset-0 pointer-events-none">
                <div className="absolute top-[-20%] left-[-10%] w-[500px] h-[500px] rounded-full bg-glow-blue/5 blur-[120px]" />
                <div className="absolute bottom-[-20%] right-[-10%] w-[600px] h-[600px] rounded-full bg-glow-purple/5 blur-[120px]" />
            </div>

            {/* Back button */}
            <Button
                variant="ghost"
                className="absolute top-6 left-6 z-10"
                onClick={() => navigate("/")}
            >
                <ArrowLeft className="mr-2 h-4 w-4" />
                Back
            </Button>

            {/* Login Card */}
            <div className="relative z-10 w-full max-w-md px-6">
                <div className="glass-card p-8 md:p-10 animate-fade-in">
                    {/* Logo */}
                    <div className="flex items-center justify-center gap-2 mb-8">
                        <Activity className="h-8 w-8 text-primary" />
                        <span className="text-2xl font-bold">RetailNexus</span>
                    </div>

                    {/* Title */}
                    <div className="text-center mb-8">
                        <h1 className="text-3xl font-bold mb-2">Welcome Back</h1>
                        <p className="text-muted-foreground">
                            Sign in to access your retail analytics dashboard
                        </p>
                    </div>

                    {/* Form */}
                    <form onSubmit={handleSubmit} className="space-y-6">
                        <div className="space-y-2">
                            <Label htmlFor="username">Username</Label>
                            <Input
                                id="username"
                                type="text"
                                placeholder="Enter your username"
                                value={username}
                                onChange={(e) => setUsername(e.target.value)}
                                required
                                className="bg-background/50"
                            />
                        </div>

                        <div className="space-y-2">
                            <Label htmlFor="password">Password</Label>
                            <Input
                                id="password"
                                type="password"
                                placeholder="Enter your password"
                                value={password}
                                onChange={(e) => setPassword(e.target.value)}
                                required
                                className="bg-background/50"
                            />
                        </div>

                        <Button
                            type="submit"
                            className="w-full bg-primary hover:bg-primary/90 glow-blue"
                            disabled={isLoading}
                        >
                            {isLoading ? (
                                "Signing in..."
                            ) : (
                                <>
                                    <LogIn className="mr-2 h-4 w-4" />
                                    Sign In
                                </>
                            )}
                        </Button>
                    </form>

                    {/* Divider */}
                    <div className="relative my-6">
                        <div className="absolute inset-0 flex items-center">
                            <div className="w-full border-t border-border/50" />
                        </div>
                        <div className="relative flex justify-center text-xs uppercase">
                            <span className="bg-card px-2 text-muted-foreground">Quick Login</span>
                        </div>
                    </div>

                    {/* Quick Login Options */}
                    <div className="grid grid-cols-2 gap-3">
                        <Button
                            type="button"
                            variant="outline"
                            className="border-amber-500/30 hover:bg-amber-500/10 hover:border-amber-500/50"
                            onClick={() => handleQuickLogin("admin", "admin123", true)}
                            disabled={isLoading}
                        >
                            <Shield className="mr-2 h-4 w-4 text-amber-500" />
                            Admin
                        </Button>

                        <Button
                            type="button"
                            variant="outline"
                            className="border-primary/30 hover:bg-primary/10"
                            onClick={() => handleQuickLogin("demo", "demo", false)}
                            disabled={isLoading}
                        >
                            <User className="mr-2 h-4 w-4" />
                            Demo
                        </Button>
                    </div>

                    {/* Footer Note */}
                    <div className="mt-6 space-y-2">
                        <p className="text-center text-xs text-muted-foreground">
                            <strong className="text-amber-500">Admin:</strong> <code className="text-primary">admin</code> / <code className="text-primary">admin123</code>
                        </p>
                        <p className="text-center text-xs text-muted-foreground">
                            <strong>Demo:</strong> <code className="text-primary">demo</code> / <code className="text-primary">demo</code>
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Login;
