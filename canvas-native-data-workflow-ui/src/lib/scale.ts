/** Live scale factor of the fixed 1600x1000 application frame vs. the viewport. */
export const scaleRef = { current: 1 };

export function toFrame(delta: number) {
  return delta / (scaleRef.current || 1);
}
