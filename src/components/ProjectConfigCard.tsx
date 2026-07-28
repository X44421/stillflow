import React, { useEffect, useRef, useState } from 'react';
import { ChevronRight, FileText, Upload, X } from '../icons/hero';

const MAX_CSV_BYTES = 50 * 1024 * 1024;

export interface ProjectConfigValues {
  name: string;
  description: string;
  datasetFile: File | null;
}

interface ProjectConfigCardProps {
  mode: 'create' | 'edit';
  initialName?: string;
  initialDescription?: string;
  busy?: boolean;
  error?: string | null;
  onCancel: () => void;
  onSubmit: (values: ProjectConfigValues) => void | Promise<void>;
}

function formatFileSize(bytes: number): string {
  if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${Math.max(1, Math.ceil(bytes / 1024))} KB`;
}

const ProjectConfigCard: React.FC<ProjectConfigCardProps> = ({
  mode,
  initialName = '',
  initialDescription = '',
  busy = false,
  error = null,
  onCancel,
  onSubmit,
}) => {
  const [name, setName] = useState(initialName);
  const [description, setDescription] = useState(initialDescription);
  const [descriptionExpanded, setDescriptionExpanded] = useState(
    Boolean(initialDescription)
  );
  const [datasetFile, setDatasetFile] = useState<File | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const descriptionInputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    setName(initialName);
    setDescription(initialDescription);
    setDescriptionExpanded(Boolean(initialDescription));
    setDatasetFile(null);
    setFileError(null);
  }, [initialDescription, initialName, mode]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !busy) onCancel();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [busy, onCancel]);

  const submitLabel = mode === 'create' ? 'Create project' : 'Save changes';
  const visibleError = fileError ?? error;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/20 p-4"
      onMouseDown={() => {
        if (!busy) onCancel();
      }}
    >
      <form
        role="dialog"
        aria-modal="true"
        aria-labelledby="project-config-title"
        className="max-h-[calc(100vh-32px)] w-full max-w-[720px] overflow-y-auto rounded-lg border border-[#e3e6e8] bg-white px-6 py-7 shadow-[0_8px_24px_rgba(32,33,36,.16)] sm:px-10 sm:py-9"
        onMouseDown={(event) => event.stopPropagation()}
        onSubmit={(event) => {
          event.preventDefault();
          const trimmedName = name.trim();
          if (!trimmedName || busy || fileError) return;
          void onSubmit({
            name: trimmedName,
            description: description.trim(),
            datasetFile: mode === 'create' ? datasetFile : null,
          });
        }}
      >
        <header className="flex min-h-10 items-center justify-between gap-4">
          <h2
            id="project-config-title"
            className="text-[22px] font-semibold leading-7 text-[#1b1d20]"
          >
            {mode === 'create' ? 'New project' : 'Project settings'}
          </h2>
          <div className="flex items-center gap-2">
            <button
              type="button"
              className="flex h-8 w-8 items-center justify-center rounded-md text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700 disabled:cursor-wait disabled:opacity-50"
              aria-label="Close project configuration"
              disabled={busy}
              onClick={onCancel}
            >
              <X size={16} />
            </button>
            <button
              type="submit"
              className="h-8 rounded-full bg-[#18181b] px-3.5 text-[13px] font-medium text-white transition-colors hover:bg-[#3f3f46] disabled:cursor-wait disabled:bg-gray-400"
              disabled={busy || !name.trim() || Boolean(fileError)}
            >
              {busy
                ? mode === 'create'
                  ? 'Creating...'
                  : 'Saving...'
                : submitLabel}
            </button>
          </div>
        </header>

        <div className="mt-9 flex flex-col gap-8">
          <label className="block">
            <span className="sr-only">Project name</span>
            <input
              autoFocus
              type="text"
              maxLength={120}
              value={name}
              onChange={(event) => setName(event.target.value)}
              className="h-10 w-full border-0 border-b border-[#e3e6e8] bg-transparent px-0 pb-2 text-[22px] font-medium leading-8 text-[#202124] outline-none placeholder:font-normal placeholder:text-gray-400 focus:border-[#18181b]"
              placeholder="Untitled project"
              autoComplete="off"
              spellCheck={false}
            />
          </label>

          <div>
            {descriptionExpanded ? (
              <textarea
                ref={descriptionInputRef}
                maxLength={1000}
                rows={3}
                value={description}
                onChange={(event) => setDescription(event.target.value)}
                onBlur={() => {
                  if (!description.trim()) setDescriptionExpanded(false);
                }}
                className="min-h-[72px] w-full resize-none rounded-lg border border-[#dadce0] bg-white px-3 py-2.5 text-[13px] leading-5 text-[#202124] outline-none transition-colors placeholder:text-gray-400 focus:border-[#18181b] focus:ring-1 focus:ring-[#18181b]"
                placeholder="Describe the goal of this project..."
              />
            ) : (
              <button
                type="button"
                className="text-[13px] text-gray-500 transition-colors hover:text-[#1b1d20]"
                onClick={() => {
                  setDescriptionExpanded(true);
                  window.requestAnimationFrame(() =>
                    descriptionInputRef.current?.focus()
                  );
                }}
              >
                + Add description
              </button>
            )}
          </div>

          {mode === 'create' && (
            <section className="border-t border-gray-100 pt-6">
              <h3 className="text-[11px] font-medium uppercase tracking-[0.08em] text-gray-500">
                Data
              </h3>

              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,text/csv"
                className="hidden"
                onChange={(event) => {
                  const file = event.target.files?.[0] ?? null;
                  event.target.value = '';
                  if (!file) return;
                  if (!file.name.toLowerCase().endsWith('.csv')) {
                    setDatasetFile(null);
                    setFileError('Only CSV files are supported.');
                    return;
                  }
                  if (file.size > MAX_CSV_BYTES) {
                    setDatasetFile(null);
                    setFileError('CSV files cannot exceed 50 MB.');
                    return;
                  }
                  setDatasetFile(file);
                  setFileError(null);
                }}
              />

              <button
                type="button"
                className="mt-3 flex h-9 w-full items-center gap-2 rounded-md px-2 text-left text-[#1b1d20] transition-colors hover:bg-gray-100 disabled:cursor-wait disabled:opacity-50"
                disabled={busy}
                onClick={() => fileInputRef.current?.click()}
              >
                <Upload size={16} className="flex-shrink-0 text-gray-500" />
                <span className="text-[13px] font-medium">Import CSV</span>
                <span className="text-[11px] text-gray-500">
                  Optional, up to 50 MB
                </span>
                <ChevronRight
                  size={16}
                  className="ml-auto flex-shrink-0 text-gray-400"
                />
              </button>

              {datasetFile && (
                <div className="mt-1 flex h-9 items-center gap-2 rounded-md bg-gray-100 px-2">
                  <FileText size={16} className="flex-shrink-0 text-gray-500" />
                  <span className="min-w-0 flex-1 truncate text-[13px] text-[#1b1d20]">
                    {datasetFile.name}
                  </span>
                  <span className="text-[11px] text-gray-500">
                    {formatFileSize(datasetFile.size)}
                  </span>
                  <button
                    type="button"
                    className="flex h-6 w-6 flex-shrink-0 items-center justify-center rounded text-gray-400 transition-colors hover:bg-gray-200 hover:text-gray-700"
                    aria-label={`Remove ${datasetFile.name}`}
                    disabled={busy}
                    onClick={() => {
                      setDatasetFile(null);
                      setFileError(null);
                    }}
                  >
                    <X size={14} />
                  </button>
                </div>
              )}
            </section>
          )}

          {visibleError && (
            <p className="text-[12px] text-red-600" role="alert">
              {visibleError}
            </p>
          )}
        </div>
      </form>
    </div>
  );
};

export default ProjectConfigCard;
