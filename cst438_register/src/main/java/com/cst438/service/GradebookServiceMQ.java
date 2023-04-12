package com.cst438.service;


import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import com.cst438.domain.CourseDTOG;
import com.cst438.domain.Enrollment;
import com.cst438.domain.EnrollmentDTO;
import com.cst438.domain.EnrollmentRepository;


public class GradebookServiceMQ extends GradebookService {
	
	@Autowired
	RabbitTemplate rabbitTemplate;
	
	@Autowired
	EnrollmentRepository enrollmentRepository;
	
	@Autowired
	Queue gradebookQueue;
	
	
	public GradebookServiceMQ() {
		System.out.println("MQ grade book service");
	}
	
	// send message to grade book service about new student enrollment in course
	@Override
	public void enrollStudent(String student_email, String student_name, int course_id) {
		 
		// TODO 
		// create EnrollmentDTO and send to gradebookQueue
		
		System.out.println("Message send to gradbook service for student "+ student_email +" " + course_id);
		EnrollmentDTO enrollmentDTO = new EnrollmentDTO();
		enrollmentDTO.course_id = course_id;
		enrollmentDTO.studentEmail = student_email;
		enrollmentDTO.studentName = student_name;
		
		System.out.println("Post to gradebook" + enrollmentDTO);
		rabbitTemplate.convertAndSend(gradebookQueue.getName(), enrollmentDTO);
		
	}
	
	@RabbitListener(queues = "registration-queue")
	public void receive(CourseDTOG courseDTOG) {
		System.out.println("Receive enrollment :" + courseDTOG);

		//TODO 
		// for each student grade in courseDTOG,  find the student enrollment entity, update the grade and save back to enrollmentRepository.
		// update data using data from gradebook
		for (CourseDTOG.GradeDTO g : courseDTOG.grades) {
			Enrollment e = enrollmentRepository.findByEmailAndCourseId(g.student_email, courseDTOG.course_id);
			if (e == null) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Enrollment not found for student " + g.student_email + " in course " + courseDTOG.course_id);
			}
			e.setCourseGrade(g.grade);
			enrollmentRepository.save(e);
            System.out.println("Updated grade for student " + g.student_email + " in course " + courseDTOG.course_id);
		}
	}

}
