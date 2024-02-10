With schedules as (
select f.id as faculty_id,concat(f.first_name," ",f.last_name) as faculty_name, cs.id as Schedule_id, cl.class_name as Class_Name,
       csb.demo_class as Demo_Class, csb.Attended_Class as Attended_Class, cs.class_date as Class_Date,Time(cs.start_time) as Class_Time,
        Dense_Rank () over ( partition by f.id order by cs.class_date asc) as Class_date_Rank,
       Row_Number() over(partition by f.id,cs.class_date order by cs.start_time asc) as Class_Number_of_Each_Day,rf.classschedule_id as RF_Classschedule_id,
       vr.overall_comments as overall_comments, vr.overall_rating as Overall_Rating, 
       concat("https://media.bambinos.live/", vr.file_name) as Video_Link,
       concat("https://www.bambinos.live/admin/recordingfeedback/", vr.id) as Feedback_Link
       
From classschedulebookings as csb 
Left join classschedules as cs on csb.classschedule_id = cs.id
Left join classes as cl on cs.class_id = cl.id 
Left join classschedulefaculties as csf on cs.id = csf.classschedule_id 
Left join faculties as f on csf.faculty_id = f.id 
Left join videorecordings as vr on cs.id = vr.classschedule_id 
Left join recordingfeedbacks as rf on cs.id = rf.classschedule_id
Where (
        f.id in (  
            With Faculty_details as 
                (
                    select f.id as faculty_id,concat(f.first_name," ",f.last_name) as Faculty_name, f.is_approved as Faculty_Active, cs.class_date as class_date, Dense_Rank () over ( partition by f.id order by cs.class_date asc) as Class_date_Rank
                    from classschedules as cs left join classschedulefaculties as csf on cs.id = csf.classschedule_id left join faculties as f on csf.faculty_id = f.id left join classschedulebookings as csb on cs.id = csb.classschedule_id 
                    where csb.is_cancelled="No"                     group by f.id, cs.class_date                    order by f.id,cs.class_date,Class_date_Rank
                ),
                Faculty_start_end_class_Date as
                (
                    select f.id as faculty_id, f.is_approved as Faculty_Active, Min(cs.class_date) as First_class, MAX(cs.class_date) as Last_class                    from classschedules as cs 
                    left join classschedulefaculties as csf on cs.id = csf.classschedule_id left join faculties as f on csf.faculty_id = f.id left join classschedulebookings as csb on cs.id = csb.classschedule_id 
                    where csb.is_cancelled="No"                     group by f.id                    order by f.id
                )
                Select f.id as faculty_id #,concat(f.first_name," ",f.last_name) as Faculty_name, f.is_approved as Faculty_Active,FD.Class_date_Rank , FSE.First_class,FSE.Last_class
                    from faculties as f left join Faculty_details as FD on f.id = FD.faculty_id                             left join Faculty_start_end_class_Date as FSE on FD.faculty_id = FSE.faculty_id
                where Class_date_Rank <=7 and f.is_approved="Yes" and  ( First_Class >= date(date_add(now(6), INTERVAL -30 day))  AND First_Class <= date(now(6)) )
                group by faculty_id,Class_date_Rank                        order by f.id,Class_date_Rank
               )   
               
                    and csb.Attended_Class="Yes"  
      )
group by f.id,cs.class_date, cs.start_time 
order by f.id,cs.class_date
)
Select  f.id as Faculty_id, concat(f.first_name," ",f.last_name) as Faculty_Name,Schedule_id, 
        Class_Name,class_date,class_time,Demo_Class, Video_Link,Feedback_Link, Class_date_Rank, Class_Number_of_Each_Day, overall_comments,Overall_Rating
        
    from faculties as f left join schedules as s on f.id = s.faculty_id 
    where f.is_approved="Yes" and Class_date_Rank in (1,2,3) and Class_Number_of_Each_Day = 1  and RF_Classschedule_id is NULL 
    group by f.id, class_date,class_time,Class_date_Rank,Class_Number_of_Each_Day
    order by class_date asc

