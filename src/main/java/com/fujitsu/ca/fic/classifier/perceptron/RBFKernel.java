package com.fujitsu.ca.fic.classifier.perceptron;

import org.apache.mahout.math.Vector;

public class RBFKernel {
    private final double sigma;

    public RBFKernel(double gamma) {
	sigma = 1.0 / (2.0 * gamma);
    }

    public double calculateScalarProduct(Vector x1, Vector x2) {
	return Math.exp((-1 * x1.minus(x2).norm(2)) / (2 * Math.pow(sigma, 2)));
    }
}
