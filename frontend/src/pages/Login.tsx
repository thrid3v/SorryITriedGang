import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Activity, ArrowLeft, Shield, User } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

const Login = () => {
    const navigate = useNavigate();
    const { loginAsAdmin, loginAsCustomer } = useAuth();
    const { toast } = useToast();
    const [isLoading, setIsLoading] = useState(false);

    const handleAdminLogin = () => {
        setIsLoading(true);
        loginAsAdmin();
        toast({
            title: "Welcome, Admin!",
            description: "Logged in as administrator with full access.",
        });
        navigate("/dashboard");
    };

    const handleCustomerLogin = () => {
        setIsLoading(true);
        loginAsCustomer();
        toast({
            title: "Welcome!",
            description: "Logged in as customer.",
        });
        navigate("/dashboard");
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
                        <h1 className="text-3xl font-bold mb-2">Welcome</h1>
                        <p className="text-muted-foreground">
                            Choose how you'd like to explore the analytics dashboard
                        </p>
                    </div>

                    {/* Login Buttons */}
                    <div className="space-y-4">
                        <Button
                            type="button"
                            className="w-full h-16 text-lg bg-gradient-to-r from-amber-500 to-amber-600 hover:from-amber-600 hover:to-amber-700 text-white shadow-[0_0_25px_rgba(245,158,11,0.3)] hover:shadow-[0_0_35px_rgba(245,158,11,0.5)] transition-all duration-300"
                            onClick={handleAdminLogin}
                            disabled={isLoading}
                        >
                            <Shield className="mr-3 h-5 w-5" />
                            Login as Admin
                        </Button>

                        <Button
                            type="button"
                            className="w-full h-16 text-lg bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-600 hover:to-blue-700 text-white shadow-[0_0_25px_rgba(59,130,246,0.3)] hover:shadow-[0_0_35px_rgba(59,130,246,0.5)] transition-all duration-300"
                            onClick={handleCustomerLogin}
                            disabled={isLoading}
                        >
                            <User className="mr-3 h-5 w-5" />
                            Login as Customer
                        </Button>
                    </div>

                    {/* Footer Note */}
                    <div className="mt-8 space-y-2 text-center">
                        <p className="text-xs text-muted-foreground">
                            <strong className="text-amber-500">Admin</strong> — Full dashboard access, file uploads, data pipeline control
                        </p>
                        <p className="text-xs text-muted-foreground">
                            <strong className="text-blue-500">Customer</strong> — Sales analytics and AI assistant
                        </p>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Login;
