import React, { useEffect, useState } from 'react';
import { X } from '../icons/hero';

export interface ProjectConfigValues {
  name: string;
  description: string;
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

  useEffect(() => {
    setName(initialName);
    setDescription(initialDescription);
  }, [initialDescription, initialName, mode]);

  const submitLabel = mode === 'create' ? 'Create project' : 'Save changes';

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/25 p-4"
      onMouseDown={() => {
        if (!busy) onCancel();
      }}
    >
      <section
        role="dialog"
        aria-modal="true"
        aria-labelledby="project-config-title"
        className="w-full max-w-[440px] rounded-lg border border-gray-200 bg-white shadow-xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between px-5 pb-4 pt-5">
          <div>
            <h2
              id="project-config-title"
              className="text-[16px] font-semibold text-gray-900"
            >
              {mode === 'create' ? 'Create project' : 'Project settings'}
            </h2>
            <p className="mt-1 text-[12px] text-gray-500">
              Projects start empty. Import a CSV to create the source node.
            </p>
          </div>
          <button
            type="button"
            className="rounded-md p-1 text-gray-400 hover:bg-gray-100 hover:text-gray-700"
            aria-label="Close project configuration"
            disabled={busy}
            onClick={onCancel}
          >
            <X size={17} />
          </button>
        </div>

        <form
          className="px-5 pb-5"
          onSubmit={(event) => {
            event.preventDefault();
            const trimmedName = name.trim();
            if (!trimmedName || busy) return;
            void onSubmit({
              name: trimmedName,
              description: description.trim(),
            });
          }}
        >
          <label className="block">
            <span className="mb-1.5 block text-[12px] font-medium text-gray-700">
              Project name
            </span>
            <input
              autoFocus
              type="text"
              maxLength={120}
              value={name}
              onChange={(event) => setName(event.target.value)}
              className="h-9 w-full rounded-lg border border-gray-200 px-3 text-[13px] text-gray-900 outline-none transition-colors placeholder:text-gray-400 focus:border-gray-400"
              placeholder="Untitled project"
            />
          </label>

          <label className="mt-4 block">
            <span className="mb-1.5 block text-[12px] font-medium text-gray-700">
              Description
            </span>
            <textarea
              maxLength={1000}
              rows={4}
              value={description}
              onChange={(event) => setDescription(event.target.value)}
              className="w-full resize-none rounded-lg border border-gray-200 px-3 py-2 text-[13px] text-gray-900 outline-none transition-colors placeholder:text-gray-400 focus:border-gray-400"
              placeholder="Optional project context"
            />
          </label>

          {error && (
            <p className="mt-3 text-[12px] text-red-600" role="alert">
              {error}
            </p>
          )}

          <div className="mt-5 flex justify-end gap-2">
            <button
              type="button"
              className="h-9 rounded-lg border border-gray-200 px-3.5 text-[13px] font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-wait disabled:opacity-50"
              disabled={busy}
              onClick={onCancel}
            >
              Cancel
            </button>
            <button
              type="submit"
              className="h-9 rounded-lg bg-gray-900 px-3.5 text-[13px] font-medium text-white hover:bg-gray-800 disabled:cursor-wait disabled:bg-gray-500"
              disabled={busy || !name.trim()}
            >
              {busy ? 'Saving...' : submitLabel}
            </button>
          </div>
        </form>
      </section>
    </div>
  );
};

export default ProjectConfigCard;
