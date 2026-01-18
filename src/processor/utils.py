import math

EARTH_RADIUS_NM = 3440.065

# Ship Type codes from AIS
VESSEL_RISK_MULTIPLIERS = {

    # High-consequence vessels 
    80: 1.5,  
    70: 1.4, 
    71: 1.4,  
    72: 1.4,  
    
    # Medium-consequence vessels
    60: 1.2,  
    69: 1.2,  
    
    # Lower-consequence vessels
    30: 0.9, 
    36: 0.8,  
    37: 0.7, 
    
    # Special cases
    50: 1.3, 
    51: 1.0,  
    52: 1.1,  
    
    # Default for unknown
    0: 1.0    
}

# Calculate collision severity based on vessel type
def calculate_severity_score(vessel_a, vessel_b):
    type_a = vessel_a.get('ship_type', 0)
    type_b = vessel_b.get('ship_type', 0)
    
    multiplier_a = VESSEL_RISK_MULTIPLIERS.get(type_a, 1.0)
    multiplier_b = VESSEL_RISK_MULTIPLIERS.get(type_b, 1.0)
    
    severity = (multiplier_a + multiplier_b) / 2.0
    
    return severity

# Haversine distance = The angular distance between two points on a sphere's surface
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return EARTH_RADIUS_NM * c

# Closest Point of Approach; Calculate the predicted closest distance & time between two vessels
def calculate_cpa(vessel_a, vessel_b):
    current_distance = haversine_distance(
        vessel_a['lat'], vessel_a['lon'],
        vessel_b['lat'], vessel_b['lon']
    )
    # Speed (SOG) = x (East-West motion), Sirection (COG) = y (North-South motion)
    va_x = vessel_a['sog'] * math.sin(math.radians(vessel_a['cog']))
    va_y = vessel_a['sog'] * math.cos(math.radians(vessel_a['cog']))
    vb_x = vessel_b['sog'] * math.sin(math.radians(vessel_b['cog']))
    vb_y = vessel_b['sog'] * math.cos(math.radians(vessel_b['cog']))
    
    vr_x = va_x - vb_x
    vr_y = va_y - vb_y
    
    # Relative position
    dlat_nm = (vessel_a['lat'] - vessel_b['lat']) * 60 # 1 deg latitiude = 60 nm
    dlon_nm = (vessel_a['lon'] - vessel_b['lon']) * 60 * math.cos(math.radians(vessel_a['lat']))
    
    relative_speed = math.sqrt(vr_x**2 + vr_y**2)
    if relative_speed < 0.1: # If stationary
        return current_distance, None
    
    # Time to CPA
    t_cpa = -(dlat_nm * vr_y + dlon_nm * vr_x) / (vr_x**2 + vr_y**2)
    if t_cpa < 0:
        return current_distance, None
    
    # Distance at CPA
    cpa_dlat = dlat_nm + vr_y * t_cpa
    cpa_dlon = dlon_nm + vr_x * t_cpa
    cpa_distance = math.sqrt(cpa_dlat**2 + cpa_dlon**2)
    
    # Return CPA in nm & time to CPA in minutes
    return cpa_distance, t_cpa * 60

def is_collision_risk(cpa_distance, time_to_cpa_minutes, vessel_a, vessel_b):
    if cpa_distance is None or time_to_cpa_minutes is None:
        return False
    
    # Only consider moving vessels
    if vessel_a.get('nav_status', 15) not in [0, 7, 8] or \
       vessel_b.get('nav_status', 15) not in [0, 7, 8]:
        return False
    
    severity = calculate_severity_score(vessel_a, vessel_b)
    
    # Stage 4: Immediate Danger
    if cpa_distance < 0.25 * severity and time_to_cpa_minutes < 6:
        return True
    
    # Stage 3: Close Quarters
    if cpa_distance < 0.5 * severity and time_to_cpa_minutes < 12:
        return True
    
    # Stage 2: Risk of Collision
    if cpa_distance < 1.0 * severity and time_to_cpa_minutes < 20:
        return True
    
    # Stage 1: No Risk
    return False