import { Request, Response, NextFunction } from 'express';
import { DataService } from '../services/data.service';
import { successResponse, errorResponse } from '../utils/response';

export class DataController {
  private dataService: DataService;

  constructor() {
    this.dataService = new DataService();
  }

  /**
   * Query processed clinical data
   * GET /api/data
   */
  getData = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { studyId, participantId, measurementType, startDate, endDate } = req.query;
      
      const filters = {
        studyId: studyId as string,
        participantId: participantId as string,
        measurementType: measurementType as string,
        startDate: startDate as string,
        endDate: endDate as string
      };

      const data = await this.dataService.getData(filters);
      successResponse(res, data, 'Data retrieved successfully');
    } catch (error) {
      next(error);
    }
  };

  /**
   * Get study data
   * GET /api/data/studies/:id
   */
  getStudyData = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { id } = req.params;
      const data = await this.dataService.getStudyData(id);
      
      if (!data || data.length === 0) {
        errorResponse(res, 'Study not found', 404);
        return;
      }

      successResponse(res, data, 'Study data retrieved successfully');
    } catch (error) {
      next(error);
    }
  };
}
