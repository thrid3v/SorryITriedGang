import { useState, useRef, useCallback } from "react";
import { Upload, X, CheckCircle, Loader2, AlertCircle, FileSpreadsheet } from "lucide-react";
import { Button } from "@/components/ui/button";
import { scanFile, processFile } from "@/data/api";
import ColumnMapper from "../ingestion/ColumnMapper";

interface DataUploaderProps {
    onUploadComplete?: () => void;
}

type UploadStep = "upload" | "map" | "processing" | "complete";

const DataUploader = ({ onUploadComplete }: DataUploaderProps) => {
    const [isDragOver, setIsDragOver] = useState(false);
    const [file, setFile] = useState<File | null>(null);
    const [step, setStep] = useState<UploadStep>("upload");
    const [scanResult, setScanResult] = useState<any>(null);
    const [uploading, setUploading] = useState(false);
    const [result, setResult] = useState<{
        status: "success" | "error";
        message: string;
        tables?: string[];
        rows?: number;
    } | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null);

    const handleDragOver = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        setIsDragOver(true);
    }, []);

    const handleDragLeave = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        setIsDragOver(false);
    }, []);

    const handleDrop = useCallback((e: React.DragEvent) => {
        e.preventDefault();
        setIsDragOver(false);
        const droppedFile = e.dataTransfer.files[0];
        if (droppedFile) {
            validateAndSetFile(droppedFile);
        }
    }, []);

    const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
        const selectedFile = e.target.files?.[0];
        if (selectedFile) {
            validateAndSetFile(selectedFile);
        }
    };

    const validateAndSetFile = (f: File) => {
        const validExtensions = [".csv", ".xlsx", ".xls", ".tsv", ".parquet", ".json"];
        const ext = f.name.substring(f.name.lastIndexOf(".")).toLowerCase();
        if (!validExtensions.includes(ext)) {
            setResult({
                status: "error",
                message: `Unsupported file type "${ext}". Please upload CSV, TSV, Excel, Parquet, or JSON files.`,
            });
            return;
        }
        setFile(f);
        setResult(null);
    };

    const handleScan = async () => {
        if (!file) return;

        setUploading(true);
        setResult(null);

        try {
            const response = await scanFile(file);
            setScanResult(response);
            setStep("map");
        } catch (err: any) {
            setResult({
                status: "error",
                message: err.message || "Scan failed",
            });
        } finally {
            setUploading(false);
        }
    };

    const handleProcess = async (mapping: Record<string, string>, fileType: string) => {
        if (!scanResult) return;

        setStep("processing");
        setUploading(true);
        setResult(null);

        try {
            const response = await processFile({
                filename: scanResult.filename,
                file_type: fileType,
                mapping: mapping,
            });

            setResult({
                status: "success",
                message: response.message,
                rows: response.rows,
            });
            setStep("complete");
            setFile(null);
            setScanResult(null);
            if (fileInputRef.current) {
                fileInputRef.current.value = "";
            }
            // Refresh dashboard data
            onUploadComplete?.();
        } catch (err: any) {
            setResult({
                status: "error",
                message: err.message || "Processing failed",
            });
            setStep("map");
        } finally {
            setUploading(false);
        }
    };

    const handleCancel = () => {
        setStep("upload");
        setFile(null);
        setScanResult(null);
        setResult(null);
        if (fileInputRef.current) {
            fileInputRef.current.value = "";
        }
    };

    const clearFile = () => {
        setFile(null);
        setResult(null);
        if (fileInputRef.current) {
            fileInputRef.current.value = "";
        }
    };

    // Step 1: Upload
    if (step === "upload") {
        return (
            <div className="space-y-4">
                {/* Drop Zone */}
                <div
                    onDragOver={handleDragOver}
                    onDragLeave={handleDragLeave}
                    onDrop={handleDrop}
                    onClick={() => fileInputRef.current?.click()}
                    className={`
                        relative border-2 border-dashed rounded-xl p-6 text-center cursor-pointer
                        transition-all duration-300 ease-in-out
                        ${isDragOver
                            ? "border-primary bg-primary/10 scale-[1.02]"
                            : "border-border/50 hover:border-primary/50 hover:bg-muted/30"
                        }
                    `}
                >
                    <input
                        ref={fileInputRef}
                        type="file"
                        accept=".csv,.tsv,.xlsx,.xls,.parquet,.json"
                        onChange={handleFileSelect}
                        className="hidden"
                    />
                    <Upload className={`mx-auto h-8 w-8 mb-3 transition-colors ${isDragOver ? "text-primary" : "text-muted-foreground"}`} />
                    <p className="text-sm font-medium">
                        {isDragOver ? "Drop your file here" : "Drag & drop a dataset, or click to browse"}
                    </p>
                    <p className="text-xs text-muted-foreground mt-1">
                        Supports CSV, TSV, Excel, Parquet, JSON — any schema
                    </p>
                </div>

                {/* Selected File */}
                {file && (
                    <div className="flex items-center justify-between p-3 rounded-lg bg-muted/50 border border-border/50">
                        <div className="flex items-center gap-3">
                            <FileSpreadsheet className="h-5 w-5 text-primary" />
                            <div>
                                <p className="text-sm font-medium truncate max-w-[200px]">{file.name}</p>
                                <p className="text-xs text-muted-foreground">
                                    {(file.size / 1024).toFixed(1)} KB
                                </p>
                            </div>
                        </div>
                        <div className="flex items-center gap-2">
                            <Button
                                size="sm"
                                onClick={(e) => { e.stopPropagation(); handleScan(); }}
                                disabled={uploading}
                                className="bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-600 hover:to-blue-700"
                            >
                                {uploading ? (
                                    <>
                                        <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                                        Scanning...
                                    </>
                                ) : (
                                    <>
                                        <Upload className="mr-1.5 h-3.5 w-3.5" />
                                        Scan Headers
                                    </>
                                )}
                            </Button>
                            <Button
                                size="sm"
                                variant="ghost"
                                onClick={(e) => { e.stopPropagation(); clearFile(); }}
                                disabled={uploading}
                            >
                                <X className="h-4 w-4" />
                            </Button>
                        </div>
                    </div>
                )}

                {/* Result */}
                {result && (
                    <div className={`flex items-start gap-3 p-3 rounded-lg border ${result.status === "success"
                            ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400"
                            : "bg-red-500/10 border-red-500/30 text-red-400"
                        }`}>
                        {result.status === "success" ? (
                            <CheckCircle className="h-5 w-5 mt-0.5 flex-shrink-0" />
                        ) : (
                            <AlertCircle className="h-5 w-5 mt-0.5 flex-shrink-0" />
                        )}
                        <div className="text-sm">
                            <p className="font-medium">{result.message}</p>
                            {result.rows && (
                                <p className="text-xs mt-1 opacity-80">
                                    {result.rows?.toLocaleString()} rows processed
                                </p>
                            )}
                        </div>
                    </div>
                )}
            </div>
        );
    }

    // Step 2: Map Columns
    if (step === "map" && scanResult) {
        return (
            <div className="space-y-4">
                <div className="text-sm text-muted-foreground">
                    <p className="font-medium">File: {scanResult.filename}</p>
                    <p className="text-xs mt-1">{scanResult.row_count} rows detected</p>
                </div>
                <ColumnMapper
                    fileHeaders={scanResult.headers}
                    recommendedMapping={scanResult.recommended_mapping}
                    detectedType={scanResult.detected_type}
                    onConfirm={handleProcess}
                    onCancel={handleCancel}
                />
            </div>
        );
    }

    // Step 3: Processing
    if (step === "processing") {
        return (
            <div className="flex flex-col items-center justify-center p-8 space-y-4">
                <Loader2 className="h-12 w-12 animate-spin text-primary" />
                <p className="text-sm font-medium">Processing file and running pipeline...</p>
            </div>
        );
    }

    // Step 4: Complete
    if (step === "complete" && result) {
        return (
            <div className="space-y-4">
                <div className={`flex items-start gap-3 p-4 rounded-lg border ${result.status === "success"
                        ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400"
                        : "bg-red-500/10 border-red-500/30 text-red-400"
                    }`}>
                    {result.status === "success" ? (
                        <CheckCircle className="h-6 w-6 mt-0.5 flex-shrink-0" />
                    ) : (
                        <AlertCircle className="h-6 w-6 mt-0.5 flex-shrink-0" />
                    )}
                    <div className="text-sm flex-1">
                        <p className="font-medium">{result.message}</p>
                        {result.rows && (
                            <p className="text-xs mt-1 opacity-80">
                                {result.rows?.toLocaleString()} rows processed
                            </p>
                        )}
                    </div>
                </div>
                <Button
                    onClick={() => setStep("upload")}
                    variant="outline"
                    className="w-full"
                >
                    Upload Another File
                </Button>
            </div>
        );
    }

    return null;
};

export default DataUploader;
