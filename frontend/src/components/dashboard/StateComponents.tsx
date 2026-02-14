import { AlertCircle, RefreshCw, Package } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ErrorStateProps {
    title?: string;
    message: string;
    onRetry?: () => void;
}

export const ErrorState = ({ title = "Unable to load data", message, onRetry }: ErrorStateProps) => {
    return (
        <div className="flex flex-col items-center justify-center min-h-[400px] p-8 text-center">
            <div className="rounded-full bg-destructive/10 p-4 mb-4">
                <AlertCircle className="h-8 w-8 text-destructive" />
            </div>
            <h3 className="text-lg font-semibold mb-2">{title}</h3>
            <p className="text-muted-foreground max-w-md mb-6">{message}</p>
            {onRetry && (
                <Button onClick={onRetry} variant="outline" className="gap-2">
                    <RefreshCw className="h-4 w-4" />
                    Try Again
                </Button>
            )}
        </div>
    );
};

export const LoadingState = ({ message = "Loading data..." }: { message?: string }) => {
    return (
        <div className="flex flex-col items-center justify-center min-h-[400px] p-8 text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-acid-lime mb-4"></div>
            <p className="text-muted-foreground">{message}</p>
        </div>
    );
};

export const EmptyState = ({ title, message }: { title: string; message: string }) => {
    return (
        <div className="flex flex-col items-center justify-center min-h-[400px] p-8 text-center">
            <div className="rounded-full bg-muted p-4 mb-4">
                <Package className="h-8 w-8 text-muted-foreground" />
            </div>
            <h3 className="text-lg font-semibold mb-2">{title}</h3>
            <p className="text-muted-foreground max-w-md">{message}</p>
        </div>
    );
};
