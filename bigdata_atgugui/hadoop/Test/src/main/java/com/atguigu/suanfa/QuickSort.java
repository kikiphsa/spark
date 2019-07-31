package com.atguigu.suanfa;

/**
 * Create by chenqinping on 2019/6/24 9:10
 */
public class QuickSort {
    /**
     * 根据下标交换数组的两个元素
     *
     * @param arr    数组
     * @param index1 下标1
     * @param index2 下标2
     */
    public static void swap(int[] arr, int index1, int index2) {
        int temp = arr[index1];
        arr[index1] = arr[index2];
        arr[index2] = temp;
    }

    /**
     * 递归循环实现快排
     *
     * @param arr        数组
     * @param startIndex 快排的开始下标
     * @param endIndex   快排的结束下标
     */
    public static void quickSort(int[] arr, int startIndex, int endIndex) {
        if (arr != null && arr.length > 0) {
            int start = startIndex, end = endIndex;
            //target是本次循环要排序的元素，每次循环都是确定一个元素的排序位置，这个元素都是开始下标对应的元素
            int target = arr[startIndex];
            //开始循环，从两头往中间循环，相遇后循环结束
            while (start < end) {
                //从右向左循环比较，如果比target小，就和target交换位置，让所有比target小的元素到target的左边去
                while (start < end) {
                    if (arr[end] < target) {
                        swap(arr, start, end);
                        break;
                    } else {
                        end--;
                    }
                }

                //从左向右循环比较，如果比target大，就和target交换位置，让所有比target大的元素到target的右边去
                while (start < end) {
                    if (arr[start] > target) {
                        swap(arr, start, end);
                        break;
                    } else {
                        start++;
                    }
                }
            }
            //确定target的排序后，如果target左边还有元素，继续递归排序
            if ((start - 1) > startIndex) {
                quickSort(arr, startIndex, start - 1);
            }
            //确定target的排序后，如果target右边还有元素，继续递归排序
            if ((end + 1) < endIndex) {
                quickSort(arr, end + 1, endIndex);
            }
        }
    }

    public static void main(String[] args) {
        int[] arr = new int[]{4, 1, 8, 5, 3, 2, 9, 10, 6, 7};
        quickSort(arr, 0, 9);
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + ",");
        }
    }

}

