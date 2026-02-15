import React, { createContext, useContext, useState, ReactNode } from 'react';

export type UserRole = 'admin' | 'user';

export interface User {
    username: string;
    role: UserRole;
    displayName?: string;
}

interface AuthContextType {
    isAuthenticated: boolean;
    login: (username: string, password: string) => Promise<boolean>;
    logout: () => void;
    user: User | null;
    isAdmin: () => boolean;
    hasRole: (role: UserRole) => boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

// Demo user credentials with roles
const DEMO_USERS: Record<string, { password: string; role: UserRole; displayName: string }> = {
    admin: { password: 'admin123', role: 'admin', displayName: 'Administrator' },
    demo: { password: 'demo', role: 'user', displayName: 'Demo User' },
    user: { password: 'user123', role: 'user', displayName: 'Regular User' },
};

export const AuthProvider = ({ children }: { children: ReactNode }) => {
    const [isAuthenticated, setIsAuthenticated] = useState<boolean>(() => {
        // Check if user was previously authenticated
        return localStorage.getItem('isAuthenticated') === 'true';
    });
    const [user, setUser] = useState<User | null>(() => {
        const savedUser = localStorage.getItem('user');
        return savedUser ? JSON.parse(savedUser) : null;
    });

    const login = async (username: string, password: string): Promise<boolean> => {
        // Check against demo users
        const demoUser = DEMO_USERS[username.toLowerCase()];

        if (demoUser && demoUser.password === password) {
            const userData: User = {
                username: username.toLowerCase(),
                role: demoUser.role,
                displayName: demoUser.displayName,
            };

            setIsAuthenticated(true);
            setUser(userData);
            localStorage.setItem('isAuthenticated', 'true');
            localStorage.setItem('user', JSON.stringify(userData));
            return true;
        }

        return false;
    };

    const logout = () => {
        setIsAuthenticated(false);
        setUser(null);
        localStorage.removeItem('isAuthenticated');
        localStorage.removeItem('user');
    };

    const isAdmin = (): boolean => {
        return user?.role === 'admin';
    };

    const hasRole = (role: UserRole): boolean => {
        return user?.role === role;
    };

    return (
        <AuthContext.Provider value={{ isAuthenticated, login, logout, user, isAdmin, hasRole }}>
            {children}
        </AuthContext.Provider>
    );
};

export const useAuth = () => {
    const context = useContext(AuthContext);
    if (context === undefined) {
        throw new Error('useAuth must be used within an AuthProvider');
    }
    return context;
};
