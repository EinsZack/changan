import { Popover } from 'antd';
import { CarOutlined } from '@ant-design/icons';
import type { CSSProperties, ReactNode } from 'react';
import { Link } from 'react-router-dom';
import vehicleImage from '../../../assets/fleet-node-suv-v2.png';
import { buildNodeDetailPath, ROUTES } from '../../../routes/config';
import type { NodeResourceContribution } from '../../../types/computePool';
import type { NodeSummary } from '../../../types/node';
import { NodeResourcePopover } from './NodeResourcePopover';
import {
  VEHICLE_IMAGE_RATIO,
  VEHICLE_ROOF_INSET,
  VEHICLE_TIRE_INSET,
  type FleetVehiclePoint,
} from './fleet/fleetSceneGeometry';

interface VehicleNodeProps {
  node: NodeSummary;
  displayName: string;
  pose: FleetVehiclePoint;
  mirrored: boolean;
  resource?: NodeResourceContribution;
  onActiveChange?: (nodeId: string | null) => void;
  popoverContent?: ReactNode;
  popoverClassName?: string;
  fromPath?: string;
  popoverEnabled?: boolean;
  showStatusLabel?: boolean;
  vehicleImageSrc?: string;
}

export function VehicleNode({
  node,
  displayName,
  pose,
  mirrored,
  resource,
  onActiveChange,
  popoverContent,
  popoverClassName = 'vehicle-node-popover',
  fromPath = ROUTES.dashboard,
  popoverEnabled = true,
  showStatusLabel = false,
  vehicleImageSrc = vehicleImage,
}: VehicleNodeProps) {
  const {
    x,
    y,
    carWidth,
    baseWidth,
    baseHeight,
    labelScale,
    zIndex,
    depth,
    imageRatio = VEHICLE_IMAGE_RATIO,
    tireInset = VEHICLE_TIRE_INSET,
    roofInset = VEHICLE_ROOF_INSET,
    anchorX = 0.5,
    anchorY = (imageRatio - tireInset) / imageRatio,
    labelOffsetX = 0,
    labelOffsetY = 0,
    parkingFrame,
  } = pose;
  const vehicleHeight = carWidth * imageRatio;
  const left = x - carWidth * anchorX;
  const top = y - vehicleHeight * anchorY;
  const style = {
    left,
    top,
    width: carWidth,
    height: vehicleHeight,
    zIndex,
    '--vehicle-label-font': `${Math.max(14, 17 * labelScale)}px`,
    '--vehicle-label-padding': `${Math.round(20 * labelScale)}px`,
    '--vehicle-roof-inset': `${carWidth * roofInset}px`,
    '--vehicle-label-offset-x': `${labelOffsetX}px`,
    '--vehicle-label-offset-y': `${labelOffsetY}px`,
  } as CSSProperties;

  const vehicleLink = (
    <Link
      className={`vehicle-node${mirrored ? ' vehicle-node--mirror' : ''}`}
      style={style}
      data-fleet-node-id={node.id}
      data-parking-slot={pose.slotId}
      data-ground-x={x}
      data-ground-y={y}
      data-anchor-x={anchorX}
      data-anchor-y={anchorY}
      data-base-width={baseWidth}
      data-base-height={baseHeight}
      data-depth={depth}
      to={buildNodeDetailPath(node.id)}
      state={{ from: fromPath }}
      aria-label={`查看 ${displayName}（真实节点 ${node.name}）详情`}
      onMouseEnter={() => onActiveChange?.(node.id)}
      onMouseLeave={() => onActiveChange?.(null)}
      onFocus={() => onActiveChange?.(node.id)}
      onBlur={() => onActiveChange?.(null)}
    >
      {parkingFrame && (
        <span
          className="vehicle-node__parking-hit"
          aria-hidden="true"
          style={{
            left: parkingFrame.x - left,
            top: parkingFrame.y - top,
            width: parkingFrame.width,
            height: parkingFrame.height,
          }}
        />
      )}
      <strong className="vehicle-node__label" title={displayName}>
        {showStatusLabel ? <CarOutlined aria-hidden="true" /> : null}
        <span>{displayName}</span>
        {showStatusLabel ? (
          <small>
            <i aria-hidden="true" />
            在线
          </small>
        ) : null}
      </strong>
      <img className="vehicle-node__image" src={vehicleImageSrc} alt="" draggable={false} />
    </Link>
  );

  if (!popoverEnabled) return vehicleLink;

  return (
    <Popover
      overlayClassName={popoverClassName}
      placement="top"
      trigger={['hover', 'focus']}
      mouseEnterDelay={0.12}
      destroyOnHidden
      content={
        popoverContent ?? (
          <NodeResourcePopover node={node} displayName={displayName} resource={resource} />
        )
      }
    >
      {vehicleLink}
    </Popover>
  );
}
