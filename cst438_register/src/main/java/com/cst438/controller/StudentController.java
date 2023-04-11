package com.cst438.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import com.cst438.domain.Student;
import com.cst438.domain.StudentDTO;


import com.cst438.domain.StudentRepository;

@RestController
@CrossOrigin(origins = { "http://localhost:3000" })
public class StudentController {

	@Autowired
	StudentRepository studentRepository;

	@PostMapping("/student")
	@Transactional
	public StudentDTO addStudent(@RequestBody StudentDTO dto) {
		// verify that student email does not exist in database
		Student s = studentRepository.findByEmail(dto.email);
		if (s == null) {
			// create and save new student.
			// statusCode of 0 means there is no hold on registration.
			s = new Student();
			s.setEmail(dto.email);
			s.setName(dto.name);
			s.setStatus(dto.status);
			s.setStatusCode(dto.statusCode);
			s = studentRepository.save(s);
			dto.id = s.getStudent_id();
			return dto;
		} else {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Student email already exists.");
		}
	}
}
