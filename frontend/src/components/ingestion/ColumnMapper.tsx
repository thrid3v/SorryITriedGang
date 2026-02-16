import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { Card } from "@/components/ui/card";
import { ArrowRight, CheckCircle2 } from "lucide-react";

interface ColumnMapperProps {
    fileHeaders: string[];
    recommendedMapping: Record<string, string>;
    detectedType: string;
    onConfirm: (mapping: Record<string, string>, fileType: string) => void;
    onCancel: () => void;
}

const ColumnMapper = ({
    fileHeaders,
    recommendedMapping,
    detectedType,
    onConfirm,
    onCancel,
}: ColumnMapperProps) => {
    const [mapping, setMapping] = useState<Record<string, string>>(recommendedMapping);
    const [fileType, setFileType] = useState(detectedType);

    // Define required columns per file type
    const requiredColumns: Record<string, { name: string; required: boolean }[]> = {
        transactions: [
            { name: "transaction_id", required: true },
            { name: "user_id", required: false },
            { name: "product_id", required: false },
            { name: "timestamp", required: false },
            { name: "amount", required: true },
            { name: "store_id", required: false },
            { name: "quantity", required: false },
        ],
        users: [
            { name: "user_id", required: true },
            { name: "name", required: false },
            { name: "email", required: false },
            { name: "city", required: false },
            { name: "signup_date", required: false },
        ],
        products: [
            { name: "product_id", required: true },
            { name: "product_name", required: false },
            { name: "category", required: false },
            { name: "price", required: false },
        ],
    };

    const targetColumns = requiredColumns[fileType] || requiredColumns.transactions;

    const handleMappingChange = (systemCol: string, userCol: string) => {
        setMapping((prev) => ({
            ...prev,
            [systemCol]: userCol,
        }));
    };

    const handleConfirm = () => {
        onConfirm(mapping, fileType);
    };

    return (
        <div className="space-y-4">
            {/* File Type Selection */}
            <Card className="p-4 bg-muted/30 border-border/50">
                <div className="flex items-center gap-3">
                    <label className="text-sm font-medium">File Type:</label>
                    <Select value={fileType} onValueChange={setFileType}>
                        <SelectTrigger className="w-[200px]">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="transactions">Transactions</SelectItem>
                            <SelectItem value="users">Users</SelectItem>
                            <SelectItem value="products">Products</SelectItem>
                        </SelectContent>
                    </Select>
                </div>
            </Card>

            {/* Column Mapping Table */}
            <Card className="p-4 bg-card/50 border-border/50">
                <div className="space-y-3">
                    <div className="flex items-center gap-2 text-sm font-medium text-muted-foreground pb-2 border-b border-border/50">
                        <div className="flex-1">System Column</div>
                        <ArrowRight className="h-4 w-4" />
                        <div className="flex-1">Your Column</div>
                    </div>

                    {targetColumns.map((col) => (
                        <div key={col.name} className="flex items-center gap-3">
                            <div className="flex-1 flex items-center gap-2">
                                <span className="text-sm font-mono">
                                    {col.name}
                                </span>
                                {col.required && (
                                    <span className="text-xs text-red-400">*</span>
                                )}
                            </div>
                            <ArrowRight className="h-4 w-4 text-muted-foreground" />
                            <div className="flex-1">
                                <Select
                                    // When no column is selected, leave value undefined so the placeholder is shown.
                                    value={mapping[col.name] ?? undefined}
                                    onValueChange={(value) =>
                                        handleMappingChange(col.name, value)
                                    }
                                >
                                    <SelectTrigger className="w-full">
                                        <SelectValue placeholder="Select column..." />
                                    </SelectTrigger>
                                    <SelectContent>
                                        {fileHeaders.map((header) => (
                                            <SelectItem key={header} value={header}>
                                                {header}
                                            </SelectItem>
                                        ))}
                                    </SelectContent>
                                </Select>
                            </div>
                        </div>
                    ))}
                </div>
            </Card>

            {/* Info Message */}
            <div className="text-xs text-muted-foreground bg-muted/30 p-3 rounded-lg border border-border/30">
                <CheckCircle2 className="inline h-3.5 w-3.5 mr-1.5 text-emerald-400" />
                Unmapped columns will be filled with default values. Required fields marked with *.
            </div>

            {/* Actions */}
            <div className="flex gap-3 justify-end">
                <Button variant="outline" onClick={onCancel}>
                    Cancel
                </Button>
                <Button
                    onClick={handleConfirm}
                    className="bg-gradient-to-r from-emerald-500 to-emerald-600 hover:from-emerald-600 hover:to-emerald-700"
                >
                    Process File
                </Button>
            </div>
        </div>
    );
};

export default ColumnMapper;
