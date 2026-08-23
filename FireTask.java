import java.util.concurrent.RecursiveTask;

public class FireTask extends RecursiveTask<FireMapParallel.StepResult> {
    private static final int CUTOFF = 64;

    private final FireMapParallel map;
    private final FireMapParallel.Mode mode;
    private final int rowStart, rowEnd, colStart, colEnd;

    public FireTask(FireMapParallel map, FireMapParallel.Mode mode,
                    int startRow, int rowEnd, int colStart, int colEnd) {
        this.map = map;
        this.mode = mode;
        this.rowStart = startRow;
        this.rowEnd = rowEnd;
        this.colStart = colStart;
        this.colEnd = colEnd;
    }

    @Override
    protected FireMapParallel.StepResult compute() {
        int rowCount = rowEnd - rowStart;

        if (rowCount <= CUTOFF) {
            return map.updateRegion(mode, rowStart, rowEnd, colStart, colEnd);
        }

        int midRow = rowStart + rowCount / 2;

        FireTask topHalf = new FireTask(map, mode, rowStart, midRow, colStart, colEnd);
        FireTask bottomHalf = new FireTask(map, mode, midRow, rowEnd, colStart, colEnd);

        topHalf.fork();
        FireMapParallel.StepResult bottomHalfResult = bottomHalf.compute();
        FireMapParallel.StepResult topHalfResult = topHalf.join();

        return FireMapParallel.StepResult.combine(topHalfResult, bottomHalfResult);
    }
}
