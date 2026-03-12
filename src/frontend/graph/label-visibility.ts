/**
 * Label Visibility Module
 *
 * Controls how node labels are displayed with privacy options.
 * Obfuscated text is replaced with '#' characters.
 */

/** Label visibility modes */
export type LabelVisibilityMode = "normal" | "blur-domain" | "blur-all" | "hidden";

/** All modes in cycle order */
const MODES: LabelVisibilityMode[] = ["normal", "blur-domain", "blur-all", "hidden"];

/** Human-readable mode names */
const MODE_NAMES: Record<LabelVisibilityMode, string> = {
  normal: "Labels: Normal",
  "blur-domain": "Labels: Domain Hidden",
  "blur-all": "Labels: All Hidden",
  hidden: "Labels: Off",
};

/** Current visibility mode */
let currentMode: LabelVisibilityMode = "normal";

/** Replace each character with '#' */
function maskText(text: string): string {
  return "#".repeat(text.length);
}

/** Get the current label visibility mode */
export function getLabelVisibilityMode(): LabelVisibilityMode {
  return currentMode;
}

/** Set the label visibility mode */
export function setLabelVisibilityMode(mode: LabelVisibilityMode): void {
  currentMode = mode;
}

/** Cycle to the next label visibility mode */
export function cycleLabelVisibility(): LabelVisibilityMode {
  const currentIndex = MODES.indexOf(currentMode);
  const nextIndex = (currentIndex + 1) % MODES.length;
  currentMode = MODES[nextIndex]!;
  return currentMode;
}

/** Get human-readable name for the current mode */
export function getLabelVisibilityName(): string {
  return MODE_NAMES[currentMode];
}

/** Get human-readable name for a specific mode */
export function getModeName(mode: LabelVisibilityMode): string {
  return MODE_NAMES[mode];
}

/** Label parts for partial blur rendering */
export interface LabelParts {
  /** Text to render clearly (no blur) */
  clear: string;
  /** Text to render with blur effect */
  blurred: string;
}

/**
 * Get label parts for rendering with partial blur support.
 *
 * @param label The original label text
 * @returns Label parts object, or null if hidden
 */
export function getLabelParts(label: string | null | undefined): LabelParts | null {
  if (!label) return null;

  switch (currentMode) {
    case "normal":
      return { clear: label, blurred: "" };

    case "blur-domain": {
      const atIndex = label.indexOf("@");
      if (atIndex === -1) {
        // No domain part, show as-is
        return { clear: label, blurred: "" };
      }
      return {
        clear: label.substring(0, atIndex + 1),
        blurred: maskText(label.substring(atIndex + 1)),
      };
    }

    case "blur-all":
      return { clear: "", blurred: maskText(label) };

    case "hidden":
      return null;

    default:
      return { clear: label, blurred: "" };
  }
}
