package server

import (
	"context"
	"fmt"
	"go-noti-server/internal/nr"
	"go-noti-server/internal/rd"
	pbh "go-noti-server/protos/health"
	pb "go-noti-server/protos/notifications"
	"net"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/newrelic/go-agent/v3/integrations/nrgrpc"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type server struct {
	pb.UnimplementedNotificationServiceServer
}

type healthCheckServer struct {
	pbh.UnimplementedHealthServiceServer
}

func RunGrpcServer() {
	var (
		port     = os.Getenv("PORT")
		lis, err = net.Listen("tcp", port)
		s        = grpc.NewServer(grpc.ChainUnaryInterceptor(nrgrpc.UnaryServerInterceptor(nr.App), AuthInterceptor))
	)

	pb.RegisterNotificationServiceServer(s, &server{})
	pbh.RegisterHealthServiceServer(s, &healthCheckServer{})

	nr.Log(zerolog.InfoLevel, fmt.Sprintf("server listening at %v", lis.Addr()))

	if err != nil {
		nr.Log(zerolog.FatalLevel, fmt.Sprintf("failed to listen: %v", err))
	}

	if err := s.Serve(lis); err != nil {
		nr.Log(zerolog.FatalLevel, fmt.Sprintf("failed to serve: %v", err))
	}
}

func log(ctx context.Context, notification *pb.NotificationPackage, level zerolog.Level, msg string) {
	logger := nr.ContextLogger(ctx)
	logger.WithLevel(level).
		Str("title", notification.Title).
		Str("body", notification.Body).
		Str("path", notification.Data["path"]).
		Str("contentId", notification.Data["contentId"]).
		Str("contentType", notification.Data["contentType"]).
		Msg(nr.MsgFormatter(ctx, msg))
}

func (s *server) SendMessage(ctx context.Context, req *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	txn := newrelic.FromContext(ctx)
	notification := req.GetNotification()
	log(ctx, notification, zerolog.InfoLevel, "received notification")

	seg := txn.StartSegment("NotificationMarshalling")
	log(ctx, notification, zerolog.InfoLevel, "marshalling notification")
	marshaller := proto.MarshalOptions{Deterministic: true}
	data, err := marshaller.Marshal(notification)
	if err != nil {
		msg := fmt.Sprintf("error marshalling notification: %v", err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	seg.End()

	seg = txn.StartSegment("HashingNotification")
	log(ctx, notification, zerolog.InfoLevel, "hashing notification")
	hash := xxhash.Sum64(data)
	seg.End()

	log(ctx, notification, zerolog.InfoLevel, "adding notification hash to set")
	result, err := rd.Client.SAdd(ctx, rd.JobIdSet, hash).Result()
	if err != nil {
		msg := fmt.Sprintf("ERROR ADDING job:id TO SET: %v", err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	if result == 0 {
		msg := "PAYLOAD NOT UNIQUE"
		log(ctx, notification, zerolog.WarnLevel, msg)
		return nil, status.Errorf(codes.AlreadyExists, msg)
	}

	key := txn.GetTraceMetadata().TraceID
	log(ctx, notification, zerolog.InfoLevel, fmt.Sprintf("setting key %v to payload", key))
	_, err = rd.Client.Set(ctx, key, data, 0).Result()
	if err != nil {
		msg := fmt.Sprintf("ERROR SETTING key %v TO PAYLOAD: %v", key, err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	log(ctx, notification, zerolog.InfoLevel, fmt.Sprintf("adding %v to jobs queue", key))
	_, err = rd.Client.RPush(ctx, rd.JobsQueue, key).Result()
	if err != nil {
		msg := fmt.Sprintf("ERROR ADDING %v TO JOBS QUEUE: %v", key, err)
		log(ctx, notification, zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	return &pb.NotificationResponse{Message: "Message Received"}, nil
}

func (s *healthCheckServer) Check(ctx context.Context, req *pbh.HealthCheckRequest) (*pbh.HealthCheckResponse, error) {
	return &pbh.HealthCheckResponse{Message: "Alive"}, nil
}

func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	nr.LogWithContext(zerolog.InfoLevel, "auth intercept", ctx)

	token := extractFromContext(ctx)
	if !isTokenValid(token) {
		nr.LogWithContext(zerolog.WarnLevel, "invalid auth token", ctx)
		return nil, status.Errorf(codes.Unauthenticated, "token invalid")
	}
	return handler(ctx, req)
}

func extractFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	tokens := md.Get("Authorization")
	if len(tokens) == 0 {
		return ""
	}
	return tokens[0]
}

func isTokenValid(token string) bool {
	authed := os.Getenv("AUTHED")
	return token == authed
}
