import { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { useToast } from '@/hooks/use-toast';
import { LogIn, UserPlus } from 'lucide-react';

const Login = () => {
    const [isLogin, setIsLogin] = useState(true);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [loading, setLoading] = useState(false);

    const { login, register } = useAuth();
    const navigate = useNavigate();
    const { toast } = useToast();

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);

        try {
            if (isLogin) {
                await login(username, password);
                toast({
                    title: 'Login successful',
                    description: `Welcome back, ${username}!`,
                });
            } else {
                await register(username, password);
                toast({
                    title: 'Registration successful',
                    description: `Welcome to RetailNexus, ${username}!`,
                });
            }
            navigate('/dashboard');
        } catch (error: any) {
            toast({
                title: isLogin ? 'Login failed' : 'Registration failed',
                description: error.message || 'Please try again',
                variant: 'destructive',
            });
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-background via-background to-primary/5">
            <div className="glass-card p-8 w-full max-w-md">
                <div className="text-center mb-8">
                    <div className="flex items-center justify-center gap-2 mb-4">
                        <div className="h-10 w-10 rounded-lg bg-gradient-to-br from-primary to-secondary flex items-center justify-center">
                            <span className="text-2xl">📊</span>
                        </div>
                        <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-secondary bg-clip-text text-transparent">
                            RetailNexus
                        </h1>
                    </div>
                    <p className="text-muted-foreground">
                        {isLogin ? 'Sign in to your account' : 'Create a new account'}
                    </p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-4">
                    <div>
                        <label htmlFor="username" className="block text-sm font-medium mb-2">
                            Username
                        </label>
                        <Input
                            id="username"
                            type="text"
                            value={username}
                            onChange={(e) => setUsername(e.target.value)}
                            placeholder="Enter your username"
                            required
                            className="w-full"
                        />
                    </div>

                    <div>
                        <label htmlFor="password" className="block text-sm font-medium mb-2">
                            Password
                        </label>
                        <Input
                            id="password"
                            type="password"
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            placeholder="Enter your password"
                            required
                            className="w-full"
                        />
                    </div>

                    <Button
                        type="submit"
                        className="w-full"
                        disabled={loading}
                    >
                        {loading ? (
                            'Processing...'
                        ) : isLogin ? (
                            <>
                                <LogIn className="mr-2 h-4 w-4" />
                                Sign In
                            </>
                        ) : (
                            <>
                                <UserPlus className="mr-2 h-4 w-4" />
                                Sign Up
                            </>
                        )}
                    </Button>
                </form>

                <div className="mt-6 text-center">
                    <button
                        onClick={() => setIsLogin(!isLogin)}
                        className="text-sm text-muted-foreground hover:text-foreground transition-colors"
                    >
                        {isLogin ? (
                            <>
                                Don't have an account?{' '}
                                <span className="text-primary font-medium">Sign up</span>
                            </>
                        ) : (
                            <>
                                Already have an account?{' '}
                                <span className="text-primary font-medium">Sign in</span>
                            </>
                        )}
                    </button>
                </div>

                {isLogin && (
                    <div className="mt-6 p-4 bg-muted/30 rounded-lg">
                        <p className="text-xs text-muted-foreground text-center mb-2">Demo Credentials:</p>
                        <div className="text-xs space-y-1">
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Admin:</span>
                                <span className="font-mono">admin / admin123</span>
                            </div>
                        </div>
                    </div>
                )}

                <div className="mt-6 text-center">
                    <Link to="/" className="text-sm text-muted-foreground hover:text-foreground transition-colors">
                        ← Back to Home
                    </Link>
                </div>
            </div>
        </div>
    );
};

export default Login;
