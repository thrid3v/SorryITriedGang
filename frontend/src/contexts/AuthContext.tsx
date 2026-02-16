import React, { createContext, useContext, useState, ReactNode } from 'react';

export type UserRole = 'admin' | 'customer';

export interface User {
    username: string;
    role: UserRole;
    displayName: string;
}

interface AuthContextType {
    isAuthenticated: boolean;
    loginAsAdmin: () => void;
    loginAsCustomer: () => void;
    logout: () => void;
    user: User | null;
    isAdmin: () => boolean;
    hasRole: (role: UserRole) => boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
    const [isAuthenticated, setIsAuthenticated] = useState<boolean>(() => {
        return localStorage.getItem('isAuthenticated') === 'true';
    });
    const [user, setUser] = useState<User | null>(() => {
        const savedUser = localStorage.getItem('user');
        return savedUser ? JSON.parse(savedUser) : null;
    });

    const setAuthUser = (userData: User) => {
        setIsAuthenticated(true);
        setUser(userData);
        localStorage.setItem('isAuthenticated', 'true');
        localStorage.setItem('user', JSON.stringify(userData));
        localStorage.setItem('user_role', userData.role);
    };

    const loginAsAdmin = () => {
        setAuthUser({
            username: 'admin',
            role: 'admin',
            displayName: 'Administrator',
        });
    };

    const loginAsCustomer = () => {
        setAuthUser({
            username: 'customer',
            role: 'customer',
            displayName: 'Customer',
        });
    };

    const logout = () => {
        setIsAuthenticated(false);
        setUser(null);
        localStorage.removeItem('isAuthenticated');
        localStorage.removeItem('user');
        localStorage.removeItem('user_role');
    };

    const isAdmin = (): boolean => {
        return user?.role === 'admin';
    };

    const hasRole = (role: UserRole): boolean => {
        return user?.role === role;
    };

    return (
        <AuthContext.Provider value={{ isAuthenticated, loginAsAdmin, loginAsCustomer, logout, user, isAdmin, hasRole }}>
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
