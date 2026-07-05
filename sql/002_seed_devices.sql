-- Auto-generated from src/common/devices.py (deterministic seed=42)
-- Seeds the `devices` dimension table so raw_network_events / alerts
-- foreign keys never fail due to a missing device.

INSERT INTO devices (device_id, device_type, region) VALUES
    ('RTR-ASW-001', 'router', 'Aswan'),
    ('RTR-ASW-002', 'router', 'Aswan'),
    ('RTR-MNS-003', 'router', 'Mansoura'),
    ('RTR-SHR-004', 'router', 'Sharqia'),
    ('RTR-ALX-005', 'router', 'Alexandria'),
    ('RTR-ALX-006', 'router', 'Alexandria'),
    ('RTR-SHR-007', 'router', 'Sharqia'),
    ('RTR-GIZ-008', 'router', 'Giza'),
    ('SWT-GIZ-001', 'switch', 'Giza'),
    ('SWT-ALX-002', 'switch', 'Alexandria'),
    ('SWT-GIZ-003', 'switch', 'Giza'),
    ('SWT-ASW-004', 'switch', 'Aswan'),
    ('SWT-CAI-005', 'switch', 'Cairo'),
    ('SWT-ASW-006', 'switch', 'Aswan'),
    ('SWT-ALX-007', 'switch', 'Alexandria'),
    ('SWT-MNS-008', 'switch', 'Mansoura'),
    ('FWL-CAI-001', 'firewall', 'Cairo'),
    ('FWL-SHR-002', 'firewall', 'Sharqia'),
    ('FWL-CAI-003', 'firewall', 'Cairo'),
    ('FWL-ALX-004', 'firewall', 'Alexandria'),
    ('BST-ALX-001', 'base_station', 'Alexandria'),
    ('BST-ASW-002', 'base_station', 'Aswan'),
    ('BST-MNS-003', 'base_station', 'Mansoura'),
    ('BST-ALX-004', 'base_station', 'Alexandria'),
    ('BST-CAI-005', 'base_station', 'Cairo'),
    ('BST-GIZ-006', 'base_station', 'Giza')
ON CONFLICT (device_id) DO NOTHING;
