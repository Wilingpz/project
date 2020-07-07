#pragma once//有比较成熟的大数库boost
#include<string>
#include<utility>
#include<iostream>

class BigInt
{
public:
	//构造函数
	BigInt(){}
	BigInt(const std::string &num);
	BigInt(const int num);
	BigInt operator+(BigInt &num);
	BigInt operator-(BigInt &num);
	BigInt operator*(BigInt &num);
	BigInt operator/(BigInt &num);
	BigInt operator%(BigInt &num);
	//返回值可以做左值的返回类型为引用
	BigInt& operator+=(BigInt &num);
	BigInt& operator-=(BigInt &num);
	BigInt& operator*=(BigInt &num);
	BigInt& operator/=(BigInt &num);
	BigInt& operator%=(BigInt &num);

	BigInt& operator++();//前置++

	BigInt& operator++(int);//后置++
	BigInt& operator--();//前置--

	BigInt& operator--(int);//后置--
	friend std::ostream& operator<<(std::ostream& _cout, BigInt &num);
		//为什么输入、输出运算符的重载要写成友元函数？？？
private:
	//深浅拷贝？
	std::string add(std::string num1, std::string num2);//选中函数名按F12即可实现跳转进函数里面的操作
	std::string sub(std::string num1, std::string num2);
	std::string mul(std::string num1, std::string num2);
	std::pair<std::string,std::string> dev(std::string num1, std::string num2);//返回商和余数
	bool less(std::string& num1, std::string& num2);
	//传引用就不能less("123","456")因为这里是字符串而不是string，虽然会隐式类型转换，但是转换对象是一个匿名对象，故不能引用，除非使用const
	//大数一般通过字符串进行表示
	std::string _number;
};