/**
 * Label Visibility Module
 *
 * Controls how node labels are displayed with privacy options.
 * Uses XOR cipher with session-unique key for obfuscation.
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

/** Session-unique XOR key (generated once per page load) */
const SESSION_KEY = generateSessionKey();

/** Generate a random session key */
function generateSessionKey(): Uint8Array {
  const key = new Uint8Array(32);
  globalThis.crypto.getRandomValues(key);
  return key;
}

/** XOR cipher a string with the session key */
function xorCipher(text: string): string {
  const result: string[] = [];
  for (let i = 0; i < text.length; i++) {
    const charCode = text.charCodeAt(i);
    const keyByte = SESSION_KEY[i % SESSION_KEY.length]!;
    // XOR and map to printable ASCII range (33-126)
    const xored = charCode ^ keyByte;
    const printable = (xored % 94) + 33;
    result.push(String.fromCharCode(printable));
  }
  return result.join("");
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
        blurred: xorCipher(label.substring(atIndex + 1)),
      };
    }

    case "blur-all":
      return { clear: "", blurred: xorCipher(label) };

    case "hidden":
      return null;

    default:
      return { clear: label, blurred: "" };
  }
}
