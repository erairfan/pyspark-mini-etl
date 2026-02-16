student_id	subject
	1			A
	1			B
	1			C
	1			D
	2			A
	2			D
	2			E
	2			G
	3			E
	3			F
	3			D
	3			C
	4			A
	4			C
	4			G
	4			F
output
student_id		subject
	1			A#B#C#D
	2			A#D#E#G
	3			E#F#D#C
	4			A#C#G#F


res=df.groupBy("student_id”).agg(concat_ws(“#”,collect_list(“subject”))

