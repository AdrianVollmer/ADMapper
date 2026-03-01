/**
 * TaperedEdgeProgram: Antialiased tapered (cone-shaped) relationships.
 *
 * Based on @sigma/edge-curve's approach: render a quad, pass viewport coords
 * to fragment shader, compute distance in pixel space, apply smoothstep AA.
 */

import { EdgeProgram } from "sigma/rendering";
import type { EdgeDisplayData, NodeDisplayData, RenderParams } from "sigma/types";

/* eslint-disable no-undef */
const { UNSIGNED_BYTE, FLOAT, TRIANGLES } = WebGLRenderingContext;
/* eslint-enable no-undef */

// Float color encoding (same as sigma's internal implementation)
const INT8 = new Int8Array(4);
const INT32 = new Int32Array(INT8.buffer, 0, 1);
const FLOAT32 = new Float32Array(INT8.buffer, 0, 1);
const FLOAT_COLOR_CACHE: Record<string, number> = {};

function parseColor(val: string): { r: number; g: number; b: number; a: number } {
  let r = 0,
    g = 0,
    b = 0,
    a = 1;

  if (val[0] === "#") {
    if (val.length === 4) {
      r = parseInt(val.charAt(1) + val.charAt(1), 16);
      g = parseInt(val.charAt(2) + val.charAt(2), 16);
      b = parseInt(val.charAt(3) + val.charAt(3), 16);
    } else {
      r = parseInt(val.charAt(1) + val.charAt(2), 16);
      g = parseInt(val.charAt(3) + val.charAt(4), 16);
      b = parseInt(val.charAt(5) + val.charAt(6), 16);
    }
    if (val.length === 9) {
      a = parseInt(val.charAt(7) + val.charAt(8), 16) / 255;
    }
  } else if (/^\s*rgba?\s*\(/.test(val)) {
    const match = val.match(/^\s*rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)(?:\s*,\s*([\d.]+))?\s*\)\s*$/);
    if (match) {
      r = +(match[1] ?? 0);
      g = +(match[2] ?? 0);
      b = +(match[3] ?? 0);
      if (match[4]) a = +match[4];
    }
  }

  return { r, g, b, a };
}

function floatColor(val: string): number {
  val = val.toLowerCase();
  const cached = FLOAT_COLOR_CACHE[val];
  if (cached !== undefined) return cached;

  const { r, g, b, a } = parseColor(val);
  const alpha = (a * 255) | 0;

  INT32[0] = ((alpha << 24) | (b << 16) | (g << 8) | r) & 0xfeffffff;
  const color = FLOAT32[0] as number;

  FLOAT_COLOR_CACHE[val] = color;
  return color;
}

// Vertex shader - follows the same pattern as @sigma/edge-curve
const VERTEX_SHADER = /*glsl*/ `
attribute vec4 a_id;
attribute vec4 a_color;
attribute float a_direction;
attribute float a_thickness;
attribute vec2 a_source;
attribute vec2 a_target;
attribute float a_current;

uniform mat3 u_matrix;
uniform float u_sizeRatio;
uniform float u_pixelRatio;
uniform vec2 u_dimensions;
uniform float u_minEdgeThickness;
uniform float u_feather;

varying vec4 v_color;
varying float v_thickness;
varying float v_feather;
varying vec2 v_source;
varying vec2 v_target;

const float bias = 255.0 / 254.0;
const float epsilon = 1.5;

vec2 clipspaceToViewport(vec2 pos, vec2 dimensions) {
  return vec2(
    (pos.x + 1.0) * dimensions.x / 2.0,
    (pos.y + 1.0) * dimensions.y / 2.0
  );
}

vec2 viewportToClipspace(vec2 pos, vec2 dimensions) {
  return vec2(
    pos.x / dimensions.x * 2.0 - 1.0,
    pos.y / dimensions.y * 2.0 - 1.0
  );
}

void main() {
  float minThickness = u_minEdgeThickness;

  // Select position based on a_current (0 = target, 1 = source)
  vec2 position = a_source * max(0.0, a_current) + a_target * max(0.0, 1.0 - a_current);
  position = (u_matrix * vec3(position, 1)).xy;

  vec2 source = (u_matrix * vec3(a_source, 1)).xy;
  vec2 target = (u_matrix * vec3(a_target, 1)).xy;

  vec2 viewportPosition = clipspaceToViewport(position, u_dimensions);
  vec2 viewportSource = clipspaceToViewport(source, u_dimensions);
  vec2 viewportTarget = clipspaceToViewport(target, u_dimensions);

  vec2 delta = viewportTarget - viewportSource;
  float len = length(delta);
  vec2 unitDir = len > 0.0 ? delta / len : vec2(1.0, 0.0);
  vec2 unitNormal = vec2(-unitDir.y, unitDir.x);

  float curveThickness = max(minThickness, a_thickness / u_sizeRatio);
  v_thickness = curveThickness * u_pixelRatio;
  v_feather = u_feather;

  // Pass source and target in viewport coordinates
  v_source = viewportSource;
  v_target = viewportTarget;

  // Compute offset for this vertex
  // a_direction: -1 or +1 for the two sides of the relationship
  // The offset needs to account for the tapered shape
  float offsetAmount = (curveThickness + epsilon) * a_direction;

  vec2 viewportOffsetPosition = viewportPosition + unitNormal * offsetAmount;

  // Also extend along the relationship direction for padding at the ends
  if (a_current > 0.5) {
    // Source vertex - extend backward
    viewportOffsetPosition -= unitDir * epsilon;
  } else {
    // Target vertex - extend forward
    viewportOffsetPosition += unitDir * epsilon;
  }

  gl_Position = vec4(viewportToClipspace(viewportOffsetPosition, u_dimensions), 0, 1);

  #ifdef PICKING_MODE
  v_color = a_id;
  #else
  v_color = a_color;
  #endif

  v_color.a *= bias;
}
`;

// Fragment shader - compute distance to tapered line in pixel space
const FRAGMENT_SHADER = /*glsl*/ `
precision highp float;

varying vec4 v_color;
varying float v_thickness;
varying float v_feather;
varying vec2 v_source;
varying vec2 v_target;

const vec4 transparent = vec4(0.0, 0.0, 0.0, 0.0);

// Compute signed distance to a tapered line (cone shape)
// Negative inside, positive outside
float sdTaperedLine(vec2 p, vec2 a, vec2 b, float thickness) {
  vec2 ba = b - a;
  float len = length(ba);
  if (len < 0.001) {
    return length(p - a) - thickness * 0.5;
  }

  vec2 dir = ba / len;
  vec2 normal = vec2(-dir.y, dir.x);

  // Project point onto line
  vec2 pa = p - a;
  float along = dot(pa, dir);
  float perp = dot(pa, normal);

  // t goes from 0 (source) to 1 (target)
  float t = along / len;

  // Clamp t and compute distance based on region
  if (t < 0.0) {
    // Before source - use distance to source relationship
    float halfWidth = thickness * 0.5;
    return length(vec2(along, abs(perp) - halfWidth));
  } else if (t > 1.0) {
    // After target - use distance to target point
    return length(p - b);
  } else {
    // Along the relationship - width tapers from full at source to 0 at target
    float halfWidthAtT = thickness * 0.5 * (1.0 - t);
    return abs(perp) - halfWidthAtT;
  }
}

void main(void) {
  float dist = sdTaperedLine(gl_FragCoord.xy, v_source, v_target, v_thickness);

  float halfThickness = v_thickness / 2.0;

  // Apply antialiasing with smoothstep
  if (dist < v_feather) {
    #ifdef PICKING_MODE
    gl_FragColor = v_color;
    #else
    float t = smoothstep(-v_feather, v_feather, dist);
    gl_FragColor = mix(v_color, transparent, t);
    #endif
  } else {
    gl_FragColor = transparent;
  }
}
`;

const UNIFORMS = ["u_matrix", "u_sizeRatio", "u_pixelRatio", "u_dimensions", "u_minEdgeThickness", "u_feather"];

/**
 * Tapered relationship program with antialiasing.
 * Renders cone-shaped relationships that are wider at source and narrower at target.
 */
export class TaperedEdgeProgram extends EdgeProgram<(typeof UNIFORMS)[number], EdgeDisplayData> {
  getDefinition() {
    return {
      VERTICES: 6,
      VERTEX_SHADER_SOURCE: VERTEX_SHADER,
      FRAGMENT_SHADER_SOURCE: FRAGMENT_SHADER,
      METHOD: TRIANGLES,
      UNIFORMS,
      ATTRIBUTES: [
        { name: "a_source", size: 2, type: FLOAT },
        { name: "a_target", size: 2, type: FLOAT },
        { name: "a_thickness", size: 1, type: FLOAT },
        { name: "a_color", size: 4, type: UNSIGNED_BYTE, normalized: true },
        { name: "a_id", size: 4, type: UNSIGNED_BYTE, normalized: true },
      ],
      CONSTANT_ATTRIBUTES: [
        // a_current: 1 = source position, 0 = target position
        { name: "a_current", size: 1, type: FLOAT },
        // a_direction: -1 or +1 for perpendicular offset
        { name: "a_direction", size: 1, type: FLOAT },
      ],
      // Quad with 6 vertices (2 triangles)
      // [a_current, a_direction]
      CONSTANT_DATA: [
        [1, 1], // Source, +normal
        [1, -1], // Source, -normal
        [0, 1], // Target, +normal
        [0, 1], // Target, +normal
        [1, -1], // Source, -normal
        [0, -1], // Target, -normal
      ],
    };
  }

  processVisibleItem(
    edgeIndex: number,
    startIndex: number,
    sourceData: NodeDisplayData,
    targetData: NodeDisplayData,
    data: EdgeDisplayData
  ) {
    const array = this.array;
    const thickness = data.size || 1;

    array[startIndex++] = sourceData.x;
    array[startIndex++] = sourceData.y;
    array[startIndex++] = targetData.x;
    array[startIndex++] = targetData.y;
    array[startIndex++] = thickness;
    array[startIndex++] = floatColor(data.color);
    array[startIndex++] = edgeIndex;
  }

  setUniforms(
    params: RenderParams,
    // eslint-disable-next-line no-undef
    { gl, uniformLocations }: { gl: WebGLRenderingContext; uniformLocations: Record<string, WebGLUniformLocation> }
  ) {
    const u_matrix = uniformLocations.u_matrix!;
    const u_sizeRatio = uniformLocations.u_sizeRatio!;
    const u_pixelRatio = uniformLocations.u_pixelRatio!;
    const u_dimensions = uniformLocations.u_dimensions!;
    const u_minEdgeThickness = uniformLocations.u_minEdgeThickness!;
    const u_feather = uniformLocations.u_feather!;

    gl.uniformMatrix3fv(u_matrix, false, params.matrix);
    gl.uniform1f(u_sizeRatio, params.sizeRatio);
    gl.uniform1f(u_pixelRatio, params.pixelRatio);
    gl.uniform2f(u_dimensions, params.width * params.pixelRatio, params.height * params.pixelRatio);
    gl.uniform1f(u_minEdgeThickness, params.minEdgeThickness);
    gl.uniform1f(u_feather, params.antiAliasingFeather);
  }
}
