# Databricks notebook source
file_list = ['data.csv', 'report.txt', 'summary.csv', 'image.png', 'sales.csv','image1.jpg','photo_vacation.jpg' ,'data_report.csv' ,'sales_2025.csv' ,'notes.txt' 'todo_list.txt' ,'budget_2024.xls','employee_data.xls']
csv_files = [file for file in file_list if file.endswith('.csv')]
print(csv_files)
