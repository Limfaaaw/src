import java.util.concurrent.RecursiveTask;

public class FireTask extends RecursiveTask<FireMapParallel.StepResult> {
    private static final int CUTOFF = 64;

    private final FireMapParallel map;
    private final FireMapParallel.Mode mode;
    private final int startRow, rowEnd, colStart, colEnd;

    public FireTask(FireMapParallel map, FireMapParallel.Mode mode,
                    int startRow, int rowEnd, int colStart, int colEnd) {
        this.map = map;
        this.mode = mode;
        this.startRow = startRow;
        this.rowEnd = rowEnd;
        this.colStart = colStart;
        this.colEnd = colEnd;
    }

    @Override
    protected FireMapParallel.StepResult compute() {
        int rowCount = rowEnd - startRow;
        int colCount = colEnd - colStart;

        if (rowCount <= CUTOFF && colCount <= CUTOFF) {
            return map.updateRegion(mode, colCount, colCount, rowCount, colCount);
        }

        int midRow = startRow + rowCount / 2;
        int midCol = colStart + colCount / 2;

        FireTask topLeft = new FireTask(map, mode, startRow, midRow, colStart, midCol);
        FireTask topRight = new FireTask(map, mode, startRow, midRow, midCol, colEnd);
        FireTask bottomLeft = new FireTask(map, mode, midRow, rowEnd, colStart, midCol);
        FireTask bottomRight = new FireTask(map, mode, midRow, rowEnd, midCol, colEnd);

        topLeft.fork();
        topRight.fork();
        bottomLeft.fork();

        FireMapParallel.StepResult bottomRightResult = bottomRight.compute();
        FireMapParallel.StepResult bottomLeftResult = bottomLeft.join();
        FireMapParallel.StepResult topRightResult = topRight.join();
        FireMapParallel.StepResult topLeftResult = topLeft.join();

        FireMapParallel.StepResult topHalf = FireMapParallel.StepResult.combine(topLeftResult, topRightResult);
        FireMapParallel.StepResult bottomHalf = FireMapParallel.StepResult.combine(bottomLeftResult, bottomRightResult);    

        return FireMapParallel.StepResult.combine(topHalf, bottomHalf);
}


