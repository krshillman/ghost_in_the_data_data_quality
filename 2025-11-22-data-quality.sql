
***


--For those who want to see everything together, here's the full implementation you can adapt for your own systems:

-- Table creation with proper constraints
DROP TABLE IF EXISTS bookings CASCADE;
CREATE TABLE bookings (
    room_id INT NOT NULL,
    booking_id INT PRIMARY KEY,
    guest_name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_flag VARCHAR(50),
    CONSTRAINT valid_date_range CHECK (end_date > start_date)
);

-- Index for overlap detection performance
CREATE INDEX idx_bookings_room_dates 
ON bookings(room_id, start_date, end_date);

-- Overlap prevention trigger
CREATE OR REPLACE FUNCTION prevent_booking_overlap()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM bookings
        WHERE room_id = NEW.room_id
        AND booking_id != NEW.booking_id
        AND start_date < NEW.end_date
        AND end_date > NEW.start_date
    ) THEN
        RAISE EXCEPTION 'Booking overlap detected for room % between % and %', 
            NEW.room_id, NEW.start_date, NEW.end_date;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER validate_booking_overlap
    BEFORE INSERT OR UPDATE ON bookings
    FOR EACH ROW
    EXECUTE FUNCTION prevent_booking_overlap();

-- Data quality monitoring view
CREATE OR REPLACE VIEW daily_data_quality_report AS
WITH overlap_check AS (
    SELECT 
        'Booking Overlaps' as metric,
        COUNT(*) as issue_count,
        'CRITICAL' as severity,
        CURRENT_DATE as check_date
    FROM bookings a
    INNER JOIN bookings b
        ON a.room_id = b.room_id
        AND a.booking_id < b.booking_id
        AND a.start_date < b.end_date
        AND a.end_date > b.start_date
),
date_check AS (
    SELECT 
        'Invalid Date Ranges' as metric,
        COUNT(*) as issue_count,
        'HIGH' as severity,
        CURRENT_DATE as check_date
    FROM bookings
    WHERE end_date <= start_date
),
completeness_check AS (
    SELECT 
        'Missing Guest Names' as metric,
        COUNT(*) as issue_count,
        'MEDIUM' as severity,
        CURRENT_DATE as check_date
    FROM bookings
    WHERE guest_name IS NULL OR TRIM(guest_name) = ''
)
SELECT * FROM overlap_check
UNION ALL SELECT * FROM date_check
UNION ALL SELECT * FROM completeness_check;

-- Overlap detection query for auditing
CREATE OR REPLACE VIEW booking_overlaps AS
SELECT DISTINCT
    a.room_id,
    a.booking_id as first_booking,
    a.guest_name as first_guest,
    a.start_date as first_start,
    a.end_date as first_end,
    b.booking_id as second_booking,
    b.guest_name as second_guest,
    b.start_date as second_start,
    b.end_date as second_end,
    GREATEST(a.start_date, b.start_date) as overlap_start,
    LEAST(a.end_date, b.end_date) as overlap_end,
    LEAST(a.end_date, b.end_date) - GREATEST(a.start_date, b.start_date) as overlap_days
FROM bookings a
INNER JOIN bookings b
    ON a.room_id = b.room_id
    AND a.booking_id < b.booking_id
    AND a.start_date < b.end_date
    AND a.end_date > b.start_date
ORDER BY a.room_id, a.start_date;

-- Sample test data (will fail with trigger enabled)
-- Uncomment to test overlap prevention
/*
INSERT INTO bookings (room_id, booking_id, guest_name, start_date, end_date) VALUES
    (101, 1, 'Alice', '2024-01-01', '2024-01-10'),
    (101, 2, 'Bob', '2024-01-05', '2024-01-07'),    -- Will fail: overlaps with Alice
    (101, 3, 'Charlie', '2024-01-08', '2024-01-12'); -- Will fail: overlaps with Alice
*/


--Use this as a starting point. Adapt it for your specific domain, business rules, and infrastructure. Test thoroughly before deploying to production. And remember—the code is the easy part. The culture change is what makes it stick.