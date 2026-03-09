/**
 * Scene3D - Base 3D scene component with Three.js setup
 * 
 * This component provides a reusable 3D canvas wrapper with configurable
 * camera, lighting, and orbit controls. It supports performance mode
 * for rendering large datasets efficiently.
 */

import React, { Suspense, useMemo, useState, useEffect, useRef } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { OrbitControls, Stats, AdaptiveDpr } from '@react-three/drei';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import * as THREE from 'three';
import type { SceneConfig, Scene3DProps, PerformanceContextValue } from './types';
import { PerformanceContext } from './PerformanceContext';
import { useThemeDetection } from '../../hooks/useThemeDetection';

// Theme-aware background colors
const BACKGROUND_COLORS = {
  dark: 0x0a0a1a,  // Dark theme background
  light: 0xf1f5f9, // Light theme background (slate-100)
};

/**
 * Loading fallback component displayed while 3D content loads
 */
const LoadingFallback: React.FC = () => {
  return (
    <mesh>
      <boxGeometry args={[1, 1, 1]} />
      <meshStandardMaterial color={0x4a4a6a} wireframe />
    </mesh>
  );
};

/**
 * Scene lighting setup component
 * Adjusts lighting based on performance mode and theme
 * Optimized for glass box visualizations with reflections
 * 
 * Dark mode: Dramatic Caravaggio-style lighting with deep shadows
 * Light mode: Equally dramatic with strong directional light and soft shadows
 */
const SceneLighting: React.FC<{ performanceMode: boolean; theme: 'dark' | 'light' }> = ({ performanceMode, theme }) => {
  const isLight = theme === 'light';

  if (performanceMode) {
    return (
      <>
        <ambientLight intensity={isLight ? 0.5 : 0.3} />
        <directionalLight position={[10, 10, 5]} intensity={isLight ? 1.2 : 0.8} />
      </>
    );
  }

  if (isLight) {
    // Light mode: Same light directions as dark mode for consistent shadows
    return (
      <>
        <ambientLight intensity={0.3} />
        
        {/* Key light — same position as dark mode */}
        <directionalLight
          position={[10, 15, 10]}
          intensity={1.8}
          color="#fff5e6"
          castShadow
          shadow-mapSize={[2048, 2048]}
          shadow-camera-far={50}
          shadow-camera-left={-20}
          shadow-camera-right={20}
          shadow-camera-top={20}
          shadow-camera-bottom={-20}
          shadow-bias={-0.0001}
        />
        
        {/* Fill light — same direction as dark mode */}
        <directionalLight 
          position={[-10, 10, -10]} 
          intensity={0.5} 
          color="#93c5fd" 
        />
        
        {/* Top point light — same as dark mode */}
        <pointLight 
          position={[0, 8, 0]} 
          intensity={0.5} 
          color="#60a5fa" 
        />
        
        {/* Top spotlight — same as dark mode */}
        <spotLight
          position={[0, 15, 0]}
          angle={0.5}
          penumbra={1}
          intensity={0.8}
          color="#ffffff"
          castShadow
        />
      </>
    );
  }

  // Dark mode: Original dramatic lighting
  return (
    <>
      <ambientLight intensity={0.3} />
      <directionalLight
        position={[10, 15, 10]}
        intensity={1}
        castShadow
        shadow-mapSize={[2048, 2048]}
        shadow-camera-far={50}
        shadow-camera-left={-20}
        shadow-camera-right={20}
        shadow-camera-top={20}
        shadow-camera-bottom={-20}
      />
      <directionalLight position={[-10, 10, -10]} intensity={0.4} color="#a78bfa" />
      <pointLight position={[0, 8, 0]} intensity={0.5} color="#60a5fa" />
      <spotLight
        position={[0, 15, 0]}
        angle={0.5}
        penumbra={1}
        intensity={0.6}
        color="#ffffff"
        castShadow
      />
    </>
  );
}

/**
 * Grid helper component for spatial reference
 * Simplified in performance mode - adapts to theme
 */
const SceneGrid: React.FC<{ performanceMode: boolean; theme: 'dark' | 'light' }> = ({ performanceMode }) => {
  const gridSize = performanceMode ? 10 : 20;
  const gridDivisions = performanceMode ? 10 : 20;
  
  // Same dark grid colors for both themes
  const gridColors = [0x1e293b, 0x0f172a]; // slate-800, slate-900
  
  return (
    <gridHelper 
      args={[gridSize, gridDivisions, gridColors[0], gridColors[1]]} 
    />
  );
};

/**
 * Internal canvas content component
 * Separated to avoid re-renders of the Canvas wrapper
 */
interface CanvasContentProps {
  config: SceneConfig;
  effectivePerformanceMode: boolean;
  performanceContextValue: PerformanceContextValue;
  showStats: boolean;
  theme: 'dark' | 'light';
  children: React.ReactNode;
}

/**
 * Custom camera and controls that handles zoom manually
 */
const CameraAndControls: React.FC<{ 
  position: [number, number, number];
  effectivePerformanceMode: boolean;
  enableOrbitControls: boolean;
}> = ({ position, effectivePerformanceMode, enableOrbitControls }) => {
  const { set, size, gl, camera } = useThree();
  const controlsRef = useRef<OrbitControlsImpl>(null);
  const initializedRef = useRef(false);
  
  // Create camera once on mount
  useEffect(() => {
    if (initializedRef.current) return;
    initializedRef.current = true;
    
    // Create a proper THREE.PerspectiveCamera with all flags
    const newCamera = new THREE.PerspectiveCamera(50, size.width / size.height, 0.1, 1000);
    newCamera.position.set(...position);
    newCamera.updateProjectionMatrix();
    
    console.log('[Scene3D] Created camera with isPerspectiveCamera:', newCamera.isPerspectiveCamera);
    
    // Set as the default camera
    set({ camera: newCamera });
  }, []); // Empty deps - only run once
  
  // Handle manual zoom since OrbitControls zoom is broken
  useEffect(() => {
    if (!enableOrbitControls) return;
    
    const canvas = gl.domElement;
    
    const handleWheel = (e: WheelEvent) => {
      e.preventDefault();
      
      const zoomSpeed = 0.001;
      const delta = e.deltaY * zoomSpeed;
      
      // Get camera direction
      const direction = new THREE.Vector3();
      camera.getWorldDirection(direction);
      
      // Move camera along its direction (dolly)
      const minDistance = 2;
      const maxDistance = 100;
      
      // Calculate new position
      const newPos = camera.position.clone().addScaledVector(direction, delta * 10);
      const distanceToOrigin = newPos.length();
      
      // Clamp distance
      if (distanceToOrigin >= minDistance && distanceToOrigin <= maxDistance) {
        camera.position.copy(newPos);
      }
    };
    
    canvas.addEventListener('wheel', handleWheel, { passive: false });
    return () => canvas.removeEventListener('wheel', handleWheel);
  }, [gl, camera, enableOrbitControls]);
  
  if (!enableOrbitControls) return null;
  
  return (
    <OrbitControls
      ref={controlsRef}
      makeDefault
      enablePan={true}
      enableZoom={false} // Disable built-in zoom, we handle it manually
      enableRotate={true}
      enableDamping={!effectivePerformanceMode}
      dampingFactor={0.05}
      maxPolarAngle={Math.PI * 0.85}
      minPolarAngle={Math.PI * 0.1}
    />
  );
};

const CanvasContent: React.FC<CanvasContentProps> = ({
  config,
  effectivePerformanceMode,
  performanceContextValue,
  showStats,
  theme,
  children,
}) => {
  // Use theme-aware background color
  const backgroundColor = BACKGROUND_COLORS[theme];
  const backgroundColorHex = `#${backgroundColor.toString(16).padStart(6, '0')}`;
  
  console.log('[Scene3D] CanvasContent theme:', theme, 'backgroundColor:', backgroundColorHex);

  return (
    <>
      {/* Adaptive performance optimizations */}
      {effectivePerformanceMode && (
        <>
          <AdaptiveDpr pixelated />
        </>
      )}
      
      {/* Background color */}
      <color attach="background" args={[backgroundColorHex]} />
      
      {/* Camera and OrbitControls - camera must be ready before controls */}
      <CameraAndControls 
        position={config.cameraPosition}
        effectivePerformanceMode={effectivePerformanceMode}
        enableOrbitControls={config.enableOrbitControls}
      />
      
      {/* Scene lighting */}
      <SceneLighting performanceMode={effectivePerformanceMode} theme={theme} />
      
      {/* Performance context provider for child components */}
      <PerformanceContext.Provider value={performanceContextValue}>
        <Suspense fallback={<LoadingFallback />}>
          {children}
        </Suspense>
      </PerformanceContext.Provider>
      
      {/* Grid helper for spatial reference */}
      <SceneGrid performanceMode={effectivePerformanceMode} theme={theme} />
      
      {/* Performance stats overlay (for debugging) */}
      {showStats && <Stats />}
    </>
  );
};

/**
 * Scene3D - Base 3D scene component
 * 
 * Wraps children in a Three.js Canvas with configurable camera, lighting,
 * and orbit controls. Supports performance mode for large datasets.
 * 
 * @example
 * ```tsx
 * import { Scene3D, createSceneConfig } from './3d';
 * 
 * const config = createSceneConfig({ performanceMode: true });
 * 
 * <Scene3D config={config}>
 *   <mesh>
 *     <boxGeometry />
 *     <meshStandardMaterial color="orange" />
 *   </mesh>
 * </Scene3D>
 * ```
 */
export const Scene3D: React.FC<Scene3DProps> = ({ 
  config, 
  children,
  className = '',
  showStats = false,
  onPerformanceModeChange,
}) => {
  // Detect current theme
  const theme = useThemeDetection();
  
  // Internal performance mode state
  const [internalPerformanceMode] = useState(config.performanceMode);
  
  // Use internal state if no external callback, otherwise use config
  const effectivePerformanceMode = onPerformanceModeChange 
    ? config.performanceMode 
    : internalPerformanceMode;

  // Memoize performance context value
  const performanceContextValue = useMemo<PerformanceContextValue>(() => ({
    performanceMode: effectivePerformanceMode,
    enableAnimations: config.enableAnimations && !effectivePerformanceMode,
    // Limit elements to 1000 in performance mode
    maxElements: effectivePerformanceMode ? 1000 : 10000,
    // Reduce level of detail in performance mode
    lodFactor: effectivePerformanceMode ? 0.5 : 1,
  }), [effectivePerformanceMode, config.enableAnimations]);

  return (
    <div 
      className={`relative w-full h-full min-h-[400px] rounded-lg ${className}`}
      style={{ touchAction: 'none' }}
    >
      <Canvas
        shadows={!effectivePerformanceMode}
        dpr={effectivePerformanceMode ? 1.5 : [1, 2]}
        gl={{ 
          antialias: true, // Always enable antialiasing for crisp edges
          powerPreference: effectivePerformanceMode ? 'low-power' : 'high-performance',
        }}
        frameloop="always"
      >
        <CanvasContent
          config={config}
          effectivePerformanceMode={effectivePerformanceMode}
          performanceContextValue={performanceContextValue}
          showStats={showStats}
          theme={theme}
        >
          {children}
        </CanvasContent>
      </Canvas>
    </div>
  );
};
