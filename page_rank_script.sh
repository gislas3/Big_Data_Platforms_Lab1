#!/bin/bash

#build the matrix
hadoop jar ~/Documents/Centrale/Big_Data_Platforms/Lab1/page_rank.jar build.matrix.BuildMatrixMain /page_rank_data /page_rank_matrix

bool=true
count=0
#loop through until stationary distribution is reached
while [ "$bool" = true ]
do
	echo "ITERATION "$count
	#multiply the matrix by the vector, and store the result in vector_temp
	hadoop jar ~/Documents/Centrale/Big_Data_Platforms/Lab1/page_rank.jar multiply.matrix.MultiplyMatrixMain /page_rank_matrix /vector_temp
	#copy vector stored locally to folder with output from completed job
	hadoop fs -copyFromLocal ~/Documents/Centrale/Big_Data_Platforms/Lab1/vector.txt /vector_temp


	#compare the vectors
	hadoop jar ~/Documents/Centrale/Big_Data_Platforms/Lab1/page_rank.jar check.vector.CheckVectorMain /vector_temp /vector_diff
	DIFF="$(hadoop fs -cat /vector_diff/*)"
	#if vectors similar enough, then stop job
	if [ $DIFF -eq "1" ]  || [ $count -gt 99 ]; then
		bool=false
	fi
	echo "DIFF IS "$DIFF

	#copy to local the output from the job - modify for correct directory
	hadoop fs -cat /vector_temp/part-r-* > ~/Documents/Centrale/Big_Data_Platforms/Lab1/vector.txt 
	#remove the old vector
	hadoop fs -rm /page_rank_vector/vector.txt
	#add the new vector to the folder - modify for correct directory
	hadoop fs -copyFromLocal ~/Documents/Centrale/Big_Data_Platforms/Lab1/vector.txt /page_rank_vector
	count=$(($count + 1))

done

echo "Number of iterations was: "$count

#find/print the top 10
hadoop jar ~/Documents/Centrale/Big_Data_Platforms/Lab1/page_rank.jar output.top10.OutputTop10Main /page_rank_vector /page_rank_final_output


