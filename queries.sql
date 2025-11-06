--Which doctor has the mostconfirmed appointments?
SELECT 
    d.doctor_name,
    COUNT(a.appointment_id) AS confirmed_appointments
FROM healthtech.healthtech.appointments a
JOIN healthtech.healthtech.doctors d 
    ON a.doctor_id = d.doctor_id
WHERE a.status = 'confirmed'
GROUP BY d.doctor_name
ORDER BY confirmed_appointments DESC
LIMIT 1;

--How many confirmed appointments does the patient with patient_id '34' have?
SELECT 
    COUNT(*) AS confirmed_appointments
FROM healthtech.healthtech.appointments
WHERE patient_id = 34
  AND status = 'confirmed';


--How many cancelled appointments are there between October 21, 2025, and October 24, 2025 (inclusive)?
SELECT 
    COUNT(*) AS cancelled_appointments
FROM healthtech.healthtech.appointments
WHERE status = 'cancelled'
  AND appointment_datetime::date BETWEEN DATE '2025-10-21' AND DATE '2025-10-24';

--What is the total number of confirmed appointments for each doctor?
SELECT 
    d.doctor_name,
    COUNT(a.appointment_id) AS confirmed_appointments
FROM healthtech.healthtech.appointments a
JOIN healthtech.healthtech.doctors d 
    ON a.doctor_id = d.doctor_id
WHERE a.status = 'confirmed'
GROUP BY d.doctor_name
ORDER BY confirmed_appointments DESC;

